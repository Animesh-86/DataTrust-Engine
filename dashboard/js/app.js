/**
 * DataTrust Engine — Dashboard Controller
 *
 * Fetches scores from the backend API, renders charts,
 * and keeps everything in sync on a 30-second poll.
 */
(function () {
    'use strict';

    const API_BASE = window.location.origin;
    const POLL_INTERVAL = 30000;
    const COLORS = {
        accent: '#6366f1',
        accentLight: '#818cf8',
        green: '#22c55e',
        yellow: '#f59e0b',
        red: '#ef4444',
        text: '#e2e8f0',
        muted: '#8892b0',
        grid: 'rgba(255,255,255,0.05)',
        cardBg: 'rgba(17,20,37,0.85)'
    };

    let allScores = [];
    let distributionChart = null;
    let radarChart = null;
    let timeChart = null;
    let currentSort = { field: 'score', dir: 'desc' };

    // ---- Bootstrap ----
    document.addEventListener('DOMContentLoaded', () => {
        checkConnection();
        fetchAndRender();
        setInterval(fetchAndRender, POLL_INTERVAL);

        document.getElementById('btnRefresh').addEventListener('click', triggerRescore);
        document.getElementById('searchInput').addEventListener('input', renderTable);
        document.getElementById('gradeFilter').addEventListener('change', renderTable);
        document.getElementById('btnCloseTm').addEventListener('click', closeTimeMachine);

        // Sortable headers
        document.querySelectorAll('.sortable').forEach(th => {
            th.addEventListener('click', () => {
                const field = th.dataset.sort;
                if (currentSort.field === field) {
                    currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
                } else {
                    currentSort = { field, dir: 'desc' };
                }
                renderTable();
            });
        });
    });

    // ---- Data fetching ----
    async function fetchAndRender() {
        try {
            const [scores, stats] = await Promise.all([
                fetchJson('/api/scores'),
                fetchJson('/api/scores/stats')
            ]);

            allScores = scores || [];
            renderStats(stats);
            renderTable();
            renderDistributionChart();
            renderRadarChart();
        } catch (err) {
            console.error('Fetch failed:', err);
        }
    }

    async function fetchJson(path) {
        const resp = await fetch(API_BASE + path);
        if (!resp.ok) throw new Error(`${resp.status} from ${path}`);
        return resp.json();
    }

    async function checkConnection() {
        const badge = document.getElementById('connectionBadge');
        const pulse = badge.querySelector('.pulse');
        const text = badge.querySelector('.conn-text');
        try {
            const data = await fetchJson('/api/health');
            pulse.className = 'pulse connected';
            text.textContent = 'Connected';
        } catch {
            pulse.className = 'pulse error';
            text.textContent = 'Disconnected';
        }
    }

    async function triggerRescore() {
        const btn = document.getElementById('btnRefresh');
        btn.disabled = true;
        btn.textContent = 'Scoring...';
        try {
            await fetch(API_BASE + '/api/engine/run', { method: 'POST' });
            // Wait a moment then re-fetch
            setTimeout(() => {
                fetchAndRender();
                btn.disabled = false;
                btn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/></svg>Rescore`;
            }, 3000);
        } catch (err) {
            btn.disabled = false;
            btn.textContent = 'Retry';
        }
    }

    // ---- Stats ----
    function renderStats(stats) {
        if (!stats) return;
        setText('avgScore', stats.avgScore?.toFixed(1) || '—');
        setText('totalAssets', stats.total || 0);
        setText('healthyCount', stats.healthy || 0);
        setText('warningCount', stats.warning || 0);
        setText('criticalCount', stats.critical || 0);

        const bar = document.getElementById('avgScoreBar');
        if (bar) bar.style.width = (stats.avgScore || 0) + '%';
    }

    // ---- Table ----
    function renderTable() {
        const search = document.getElementById('searchInput').value.toLowerCase();
        const grade = document.getElementById('gradeFilter').value;

        let filtered = allScores.filter(s => {
            if (search && !s.fullyQualifiedName.toLowerCase().includes(search)
                && !(s.displayName || '').toLowerCase().includes(search)) return false;
            if (grade !== 'all' && s.grade !== grade) return false;
            return true;
        });

        // Sort
        filtered.sort((a, b) => {
            let va, vb;
            switch (currentSort.field) {
                case 'name':
                    va = (a.displayName || a.fullyQualifiedName).toLowerCase();
                    vb = (b.displayName || b.fullyQualifiedName).toLowerCase();
                    return currentSort.dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
                case 'grade':
                    va = a.grade; vb = b.grade;
                    return currentSort.dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
                default:
                    va = a.overallScore; vb = b.overallScore;
                    return currentSort.dir === 'asc' ? va - vb : vb - va;
            }
        });

        const tbody = document.getElementById('tableBody');
        if (filtered.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" class="loading-row">No scores yet — trigger a scoring run or ingest sample data into OpenMetadata.</td></tr>';
            return;
        }

        tbody.innerHTML = filtered.map(s => {
            const b = s.breakdown || {};
            return `<tr>
                <td>
                    <div class="asset-name">${escHtml(s.displayName || s.fullyQualifiedName)}</div>
                    <div class="asset-fqn">${escHtml(s.fullyQualifiedName)}</div>
                </td>
                <td class="score-cell" style="color:${scoreColor(s.overallScore)}">${s.overallScore.toFixed(1)}</td>
                <td><span class="grade-badge grade-${s.grade}">${s.grade}</span></td>
                <td>${signalCell(b.quality)}</td>
                <td>${signalCell(b.governance)}</td>
                <td>${signalCell(b.lineage)}</td>
                <td>${signalCell(b.freshness)}</td>
                <td><button class="btn-history" onclick="window.__showHistory('${escAttr(s.fullyQualifiedName)}', '${escAttr(s.displayName || s.fullyQualifiedName)}')">📈 Trend</button></td>
            </tr>`;
        }).join('');
    }

    function signalCell(val) {
        if (val == null) return '<span class="signal-val">—</span>';
        const v = Math.round(val);
        return `<div class="signal-cell">
            <div class="mini-bar"><div class="mini-bar-fill" style="width:${v}%;background:${scoreColor(v)}"></div></div>
            <span class="signal-val">${v}</span>
        </div>`;
    }

    // ---- Charts ----
    function renderDistributionChart() {
        const ctx = document.getElementById('distributionChart');
        if (!ctx) return;

        const buckets = { 'A (≥90)': 0, 'B (75–89)': 0, 'C (60–74)': 0, 'D (40–59)': 0, 'F (<40)': 0 };
        allScores.forEach(s => {
            if (s.overallScore >= 90) buckets['A (≥90)']++;
            else if (s.overallScore >= 75) buckets['B (75–89)']++;
            else if (s.overallScore >= 60) buckets['C (60–74)']++;
            else if (s.overallScore >= 40) buckets['D (40–59)']++;
            else buckets['F (<40)']++;
        });

        const data = {
            labels: Object.keys(buckets),
            datasets: [{
                data: Object.values(buckets),
                backgroundColor: [
                    COLORS.green, COLORS.accent, COLORS.yellow, '#d97706', COLORS.red
                ],
                borderWidth: 0,
                borderRadius: 6,
                barPercentage: 0.7
            }]
        };

        if (distributionChart) distributionChart.destroy();
        distributionChart = new Chart(ctx, {
            type: 'bar',
            data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        ticks: { color: COLORS.muted, font: { size: 11 } },
                        grid: { display: false }
                    },
                    y: {
                        beginAtZero: true,
                        ticks: { color: COLORS.muted, stepSize: 1, font: { size: 11 } },
                        grid: { color: COLORS.grid }
                    }
                }
            }
        });
    }

    function renderRadarChart() {
        const ctx = document.getElementById('radarChart');
        if (!ctx || allScores.length === 0) return;

        const avg = {
            quality: mean(allScores.map(s => s.breakdown?.quality || 0)),
            governance: mean(allScores.map(s => s.breakdown?.governance || 0)),
            lineage: mean(allScores.map(s => s.breakdown?.lineage || 0)),
            freshness: mean(allScores.map(s => s.breakdown?.freshness || 0))
        };

        const data = {
            labels: ['Quality', 'Governance', 'Lineage', 'Freshness'],
            datasets: [{
                data: [avg.quality, avg.governance, avg.lineage, avg.freshness],
                backgroundColor: 'rgba(99, 102, 241, 0.15)',
                borderColor: COLORS.accent,
                borderWidth: 2,
                pointBackgroundColor: COLORS.accentLight,
                pointBorderColor: '#fff',
                pointBorderWidth: 1,
                pointRadius: 5
            }]
        };

        if (radarChart) radarChart.destroy();
        radarChart = new Chart(ctx, {
            type: 'radar',
            data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    r: {
                        beginAtZero: true,
                        max: 100,
                        ticks: {
                            stepSize: 25,
                            color: COLORS.muted,
                            backdropColor: 'transparent',
                            font: { size: 10 }
                        },
                        grid: { color: COLORS.grid },
                        angleLines: { color: COLORS.grid },
                        pointLabels: {
                            color: COLORS.text,
                            font: { size: 12, weight: '600' }
                        }
                    }
                }
            }
        });
    }

    // ---- Time Machine ----
    window.__showHistory = async function (fqn, name) {
        const section = document.getElementById('timeMachineSection');
        const subtitle = document.getElementById('tmSubtitle');
        section.style.display = 'block';
        subtitle.textContent = name;
        section.scrollIntoView({ behavior: 'smooth', block: 'start' });

        try {
            const history = await fetchJson(`/api/scores/${encodeURIComponent(fqn)}/history?limit=50`);
            renderTimeChart(history.reverse(), name);
        } catch (err) {
            subtitle.textContent = 'Failed to load history';
        }
    };

    function closeTimeMachine() {
        document.getElementById('timeMachineSection').style.display = 'none';
        if (timeChart) timeChart.destroy();
    }

    function renderTimeChart(history, name) {
        const ctx = document.getElementById('timeChart');
        if (!ctx) return;

        const labels = history.map(h => {
            const d = new Date(h.computedAt);
            return d.toLocaleString('en', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        });

        const datasets = [
            { label: 'Overall', data: history.map(h => h.overallScore), borderColor: COLORS.accentLight, borderWidth: 2.5, tension: 0.3, fill: false },
            { label: 'Quality', data: history.map(h => h.breakdown?.quality || 0), borderColor: COLORS.green, borderWidth: 1.5, borderDash: [4, 4], tension: 0.3, fill: false },
            { label: 'Governance', data: history.map(h => h.breakdown?.governance || 0), borderColor: COLORS.yellow, borderWidth: 1.5, borderDash: [4, 4], tension: 0.3, fill: false },
            { label: 'Lineage', data: history.map(h => h.breakdown?.lineage || 0), borderColor: '#a78bfa', borderWidth: 1.5, borderDash: [4, 4], tension: 0.3, fill: false },
            { label: 'Freshness', data: history.map(h => h.breakdown?.freshness || 0), borderColor: COLORS.red, borderWidth: 1.5, borderDash: [4, 4], tension: 0.3, fill: false }
        ];

        if (timeChart) timeChart.destroy();
        timeChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        labels: { color: COLORS.text, usePointStyle: true, pointStyle: 'line', font: { size: 11 } }
                    }
                },
                scales: {
                    x: {
                        ticks: { color: COLORS.muted, maxRotation: 45, font: { size: 10 } },
                        grid: { display: false }
                    },
                    y: {
                        min: 0, max: 100,
                        ticks: { color: COLORS.muted, stepSize: 25, font: { size: 10 } },
                        grid: { color: COLORS.grid }
                    }
                }
            }
        });
    }

    // ---- Helpers ----
    function scoreColor(v) {
        if (v >= 70) return COLORS.green;
        if (v >= 40) return COLORS.yellow;
        return COLORS.red;
    }

    function mean(arr) {
        return arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : 0;
    }

    function setText(id, val) {
        const el = document.getElementById(id);
        if (el) el.textContent = val;
    }

    function escHtml(s) {
        if (!s) return '';
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function escAttr(s) {
        if (!s) return '';
        return s.replace(/'/g, "\\'").replace(/"/g, '&quot;');
    }
})();
