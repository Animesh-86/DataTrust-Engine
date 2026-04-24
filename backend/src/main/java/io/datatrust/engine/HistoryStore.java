package io.datatrust.engine;

import io.datatrust.model.TrustScore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Persists trust scores to SQLite for historical trend analysis.
 * One file, zero infrastructure, portable.
 */
public class HistoryStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HistoryStore.class);
    private final Connection conn;

    public HistoryStore(String dbPath) {
        try {
            this.conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            conn.setAutoCommit(true);
            initSchema();
            log.info("History store initialized at {}", dbPath);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to init SQLite at " + dbPath, e);
        }
    }

    private void initSchema() throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS trust_history (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_id    TEXT NOT NULL,
                    table_fqn   TEXT NOT NULL,
                    display_name TEXT,
                    overall     REAL NOT NULL,
                    quality     REAL,
                    governance  REAL,
                    lineage     REAL,
                    freshness   REAL,
                    grade       TEXT,
                    computed_at TEXT NOT NULL
                )
            """);
            stmt.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_fqn ON trust_history(table_fqn)");
            stmt.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_time ON trust_history(computed_at)");
        }
    }

    public void save(TrustScore score) {
        var sql = """
            INSERT INTO trust_history
                (table_id, table_fqn, display_name, overall,
                 quality, governance, lineage, freshness, grade, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        try (var ps = conn.prepareStatement(sql)) {
            ps.setString(1, score.tableId());
            ps.setString(2, score.fullyQualifiedName());
            ps.setString(3, score.displayName());
            ps.setDouble(4, score.overallScore());
            ps.setDouble(5, score.breakdown().quality());
            ps.setDouble(6, score.breakdown().governance());
            ps.setDouble(7, score.breakdown().lineage());
            ps.setDouble(8, score.breakdown().freshness());
            ps.setString(9, score.grade());
            ps.setString(10, score.computedAt().toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to save score for {}: {}", score.fullyQualifiedName(), e.getMessage());
        }
    }

    public void saveBatch(List<TrustScore> scores) {
        var sql = """
            INSERT INTO trust_history
                (table_id, table_fqn, display_name, overall,
                 quality, governance, lineage, freshness, grade, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        try {
            conn.setAutoCommit(false);
            try (var ps = conn.prepareStatement(sql)) {
                for (var score : scores) {
                    ps.setString(1, score.tableId());
                    ps.setString(2, score.fullyQualifiedName());
                    ps.setString(3, score.displayName());
                    ps.setDouble(4, score.overallScore());
                    ps.setDouble(5, score.breakdown().quality());
                    ps.setDouble(6, score.breakdown().governance());
                    ps.setDouble(7, score.breakdown().lineage());
                    ps.setDouble(8, score.breakdown().freshness());
                    ps.setString(9, score.grade());
                    ps.setString(10, score.computedAt().toString());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            log.error("Batch save failed: {}", e.getMessage());
            try { conn.rollback(); } catch (SQLException ignored) {}
        } finally {
            try { conn.setAutoCommit(true); } catch (SQLException ignored) {}
        }
    }

    /**
     * Get the last N snapshots for a specific table (for the Time Machine).
     */
    public List<TrustScore> getHistory(String tableFqn, int limit) {
        var sql = """
            SELECT * FROM trust_history
            WHERE table_fqn = ?
            ORDER BY computed_at DESC
            LIMIT ?
        """;

        var results = new ArrayList<TrustScore>();
        try (var ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableFqn);
            ps.setInt(2, limit);
            var rs = ps.executeQuery();
            while (rs.next()) {
                results.add(fromRow(rs));
            }
        } catch (SQLException e) {
            log.error("History fetch failed for {}: {}", tableFqn, e.getMessage());
        }
        return results;
    }

    /**
     * Get the most recent score for every table.
     */
    public List<TrustScore> getLatestScores() {
        var sql = """
            SELECT t.* FROM trust_history t
            INNER JOIN (
                SELECT table_fqn, MAX(computed_at) as max_time
                FROM trust_history
                GROUP BY table_fqn
            ) latest ON t.table_fqn = latest.table_fqn
                     AND t.computed_at = latest.max_time
            ORDER BY t.overall DESC
        """;

        var results = new ArrayList<TrustScore>();
        try (var stmt = conn.createStatement();
             var rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                results.add(fromRow(rs));
            }
        } catch (SQLException e) {
            log.error("Failed to fetch latest scores: {}", e.getMessage());
        }
        return results;
    }

    /** Summary stats for the dashboard header */
    public record Stats(int total, double avgScore, int healthy, int warning, int critical) {}

    public Stats getStats() {
        var latest = getLatestScores();
        int total = latest.size();
        double avg = latest.stream().mapToDouble(TrustScore::overallScore).average().orElse(0);
        int healthy = (int) latest.stream().filter(s -> s.overallScore() >= 70).count();
        int warning = (int) latest.stream().filter(s -> s.overallScore() >= 40 && s.overallScore() < 70).count();
        int critical = (int) latest.stream().filter(s -> s.overallScore() < 40).count();
        return new Stats(total, Math.round(avg * 10.0) / 10.0, healthy, warning, critical);
    }

    private TrustScore fromRow(ResultSet rs) throws SQLException {
        var breakdown = new io.datatrust.model.SignalBreakdown(
                rs.getDouble("quality"),
                rs.getDouble("governance"),
                rs.getDouble("lineage"),
                rs.getDouble("freshness")
        );
        return new TrustScore(
                rs.getString("table_id"),
                rs.getString("table_fqn"),
                rs.getString("display_name"),
                rs.getDouble("overall"),
                breakdown,
                rs.getString("grade"),
                Instant.parse(rs.getString("computed_at"))
        );
    }

    @Override
    public void close() {
        try { conn.close(); } catch (SQLException ignored) {}
    }
}
