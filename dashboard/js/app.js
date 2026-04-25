/**
 * DataTrust Engine — Bento Dashboard Controller
 *
 * Storytelling-driven: shows trust pulse, actionable fix-first
 * recommendations, and deep OM integration status.
 */
(function () {
    'use strict';

    const API = window.location.origin;
    const POLL = 30000;

    const C = {
        primary: '#1A7A6D',
        primaryMuted: '#B8DDD6',
        healthy: '#2D8B4E',
        warn: '#B8860B',
        danger: '#C0392B',
        orange: '#C2410C',
        text: '#2D2A26',
        text2: '#5C5750',
        text3: '#9C9690',
        grid: 'rgba(0,0,0,.05)',
        primaryBg: 'rgba(26,122,109,.08)',
    };

    let scores = [];
    let distChart, radar, tmChart;
    let sort = { f: 'score', d: 'desc' };
    let omUrl = 'http://localhost:8585';

    document.addEventListener('DOMContentLoaded', () => {
        init();
        setInterval(poll, POLL);
        setInterval(syncStatus, 60000);

        document.getElementById('btnRescore').addEventListener('click', rescore);
        document.getElementById('searchInput').addEventListener('input', renderTable);
        document.getElementById('gradeFilter').addEventListener('change', renderTable);
        document.getElementById('tmClose').addEventListener('click', closeTm);

        document.querySelectorAll('.sort').forEach(th => {
            th.addEventListener('click', () => {
                const f = th.dataset.sort;
                sort = sort.f === f ? { f, d: sort.d === 'asc' ? 'desc' : 'asc' } : { f, d: 'desc' };
                renderTable();
            });
        });
    });

    async function init() {
        await fetchConfig();
        checkConn();
        poll();
        syncStatus();
    }

    /* ---- Data ---- */
    async function poll() {
        try {
            const [s, st] = await Promise.all([get('/api/scores'), get('/api/scores/stats')]);
            scores = s || [];
            renderPulse(st);
            renderStats(st);
            renderTable();
            renderDist();
            renderRadar();
            renderFixes();
        } catch (e) { console.error('Poll:', e); }
    }

    async function get(p) { const r = await fetch(API + p); if (!r.ok) throw r.status; return r.json(); }

    async function fetchConfig() {
        try { const c = await get('/api/config'); omUrl = c.omServerUrl || omUrl; document.getElementById('omLink').href = omUrl; } catch {}
    }

    async function checkConn() {
        const chip = document.getElementById('connChip');
        const dot = chip.querySelector('.chip-dot');
        const label = chip.querySelector('.chip-label');
        try { await get('/api/health'); dot.className = 'chip-dot on'; label.textContent = 'Connected'; }
        catch { dot.className = 'chip-dot err'; label.textContent = 'Offline'; }
    }

    async function syncStatus() {
        try {
            const s = await get('/api/integration/status');
            const strip = document.getElementById('syncStrip');
            document.getElementById('syncProps').textContent = s.customProperties?.ready ? 'active' : 'off';
            document.getElementById('syncHook').textContent = s.webhook?.subscribed ? 'live' : 'off';
            const wbCount = s.customProperties?.lastWritebackCount || 0;
            document.getElementById('syncWb').textContent = wbCount > 0 ? wbCount + ' synced' : 'pending';
            if (s.customProperties?.ready) strip.classList.add('synced');
        } catch {}
    }

    async function rescore() {
        const b = document.getElementById('btnRescore');
        b.disabled = true; b.textContent = 'Scoring…';
        try {
            await fetch(API + '/api/engine/run', { method: 'POST' });
            setTimeout(() => { poll(); syncStatus(); b.disabled = false; b.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/></svg>Rescore'; }, 3500);
        } catch { b.disabled = false; b.textContent = 'Retry'; }
    }

    /* ---- Trust Pulse ---- */
    function renderPulse(st) {
        if (!st) return;
        const avg = st.avgScore || 0;
        document.getElementById('pulseScore').textContent = avg.toFixed(1);

        // Conic gradient
        const pct = Math.min(100, avg);
        const ring = document.getElementById('pulseRing');
        ring.style.background = `conic-gradient(${C.primary} ${pct * 3.6}deg, #E8E6E1 ${pct * 3.6}deg)`;

        // Description
        const desc = document.getElementById('pulseDesc');
        if (avg >= 70) desc.textContent = `Your data platform is in good shape. ${st.total} assets scored — ${st.healthy} are healthy.`;
        else if (avg >= 40) desc.textContent = `Trust needs attention. ${st.warning} assets in warning range, ${st.critical} critical. See "Fix First" for quick wins.`;
        else desc.textContent = `Trust is critically low across ${st.total} assets. ${st.critical} need immediate attention. Start with the "Fix First" panel.`;

        // Tags
        const tags = document.getElementById('pulseTags');
        tags.innerHTML = '';
        if (st.healthy > 0) tags.innerHTML += `<span class="ptag ptag-h">● ${st.healthy} healthy</span>`;
        if (st.warning > 0) tags.innerHTML += `<span class="ptag ptag-w">● ${st.warning} warning</span>`;
        if (st.critical > 0) tags.innerHTML += `<span class="ptag ptag-d">● ${st.critical} critical</span>`;
    }

    /* ---- Stats ---- */
    function renderStats(st) {
        if (!st) return;
        setText('stTotal', st.total || 0);
        setText('stHealthy', st.healthy || 0);
        setText('stWarn', st.warning || 0);
        setText('stCrit', st.critical || 0);
        if (st.total > 0) {
            document.getElementById('barHealthy').style.width = ((st.healthy / st.total) * 100) + '%';
        }
    }

    /* ---- Fix First ---- */
    function renderFixes() {
        const list = document.getElementById('fixList');
        if (scores.length === 0) return;

        const fixes = [];
        const noOwner = scores.filter(s => (s.breakdown?.governance || 0) < 30);
        const noDesc = scores.filter(s => {
            const b = s.breakdown || {};
            return (b.governance || 0) < 50 && (b.governance || 0) >= 25;
        });
        const lowQuality = scores.filter(s => (s.breakdown?.quality || 0) === 0);

        if (noOwner.length > 0) {
            fixes.push({
                severity: 'high',
                icon: '!',
                title: `${noOwner.length} tables have no owner`,
                desc: 'Assign ownership to improve governance score',
                impact: `+25 pts each`
            });
        }

        const undocumented = scores.filter(s => {
            const g = s.breakdown?.governance || 0;
            return g < 60;
        });
        if (undocumented.length > 0) {
            fixes.push({
                severity: 'med',
                icon: '⚡',
                title: `${undocumented.length} tables need better documentation`,
                desc: 'Add descriptions and column-level docs',
                impact: `+20 pts avg`
            });
        }

        if (lowQuality.length > 0) {
            fixes.push({
                severity: 'low',
                icon: '◎',
                title: `${lowQuality.length} tables have no quality tests`,
                desc: 'Add test cases in OpenMetadata for quality scoring',
                impact: `+15 pts each`
            });
        }

        const noTier = scores.filter(s => !s.details?.governance?.includes('✓ Tier'));
        if (noTier.length > 0) {
            fixes.push({
                severity: 'low',
                icon: '◈',
                title: `Add tier classification`,
                desc: `${noTier.length > scores.length ? scores.length : noTier.length} tables missing tier tags`,
                impact: `+15 pts`
            });
        }

        if (fixes.length === 0) {
            list.innerHTML = '<div class="fix-item"><div class="fix-icon low">✓</div><div class="fix-text"><strong>All clear!</strong><p>No urgent governance gaps found.</p></div></div>';
            return;
        }

        list.innerHTML = fixes.slice(0, 4).map(f => `
            <div class="fix-item">
                <div class="fix-icon ${f.severity}">${f.icon}</div>
                <div class="fix-text">
                    <strong>${esc(f.title)}</strong>
                    <p>${esc(f.desc)}</p>
                </div>
                <div class="fix-impact">${f.impact}</div>
            </div>
        `).join('');
    }

    /* ---- Table ---- */
    function renderTable() {
        const q = document.getElementById('searchInput').value.toLowerCase();
        const g = document.getElementById('gradeFilter').value;

        let data = scores.filter(s => {
            if (q && !s.fullyQualifiedName.toLowerCase().includes(q) && !(s.displayName || '').toLowerCase().includes(q)) return false;
            if (g !== 'all' && s.grade !== g) return false;
            return true;
        });

        data.sort((a, b) => {
            let va, vb;
            if (sort.f === 'name') { va = (a.displayName || a.fullyQualifiedName).toLowerCase(); vb = (b.displayName || b.fullyQualifiedName).toLowerCase(); return sort.d === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va); }
            if (sort.f === 'grade') { va = a.grade; vb = b.grade; return sort.d === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va); }
            va = a.overallScore; vb = b.overallScore;
            return sort.d === 'asc' ? va - vb : vb - va;
        });

        const tbody = document.getElementById('tableBody');
        if (!data.length) { tbody.innerHTML = '<tr><td colspan="8" class="empty-row">No scores yet — run a scoring cycle.</td></tr>'; return; }

        tbody.innerHTML = data.map(s => {
            const b = s.breakdown || {};
            const link = omUrl + '/table/' + encodeURIComponent(s.fullyQualifiedName);
            return `<tr class="t-row-click" data-fqn="${escA(s.fullyQualifiedName)}" style="cursor:pointer">
                <td><a class="t-name">${esc(s.displayName || s.fullyQualifiedName)}</a><div class="t-fqn">${esc(s.fullyQualifiedName)}</div></td>
                <td class="t-score" style="color:${col(s.overallScore)}">${s.overallScore.toFixed(1)}</td>
                <td><span class="grade grade-${s.grade}">${s.grade}</span></td>
                <td>${sig(b.quality)}</td>
                <td>${sig(b.governance)}</td>
                <td>${sig(b.lineage)}</td>
                <td>${sig(b.freshness)}</td>
                <td class="t-actions">
                    <button class="t-btn" onclick="event.stopPropagation();window.__tm('${escA(s.fullyQualifiedName)}','${escA(s.displayName || s.fullyQualifiedName)}')">Trend</button>
                    <a class="t-btn" href="${escA(link)}" target="_blank" onclick="event.stopPropagation()">OM↗</a>
                </td>
            </tr>`;
        }).join('');

        // Attach click handlers for detail panel
        tbody.querySelectorAll('.t-row-click').forEach(row => {
            row.addEventListener('click', () => openDetail(row.dataset.fqn));
        });
    }

    function sig(v) {
        if (v == null) return '<span class="sig-v">—</span>';
        const n = Math.round(v);
        return `<div class="sig"><div class="sig-bar"><div class="sig-bar-fill" style="width:${n}%;background:${col(n)}"></div></div><span class="sig-v">${n}</span></div>`;
    }

    /* ---- Charts ---- */
    function renderDist() {
        const ctx = document.getElementById('distChart');
        if (!ctx) return;
        const b = { 'A': 0, 'B': 0, 'C': 0, 'D': 0, 'F': 0 };
        scores.forEach(s => { if (s.overallScore >= 90) b.A++; else if (s.overallScore >= 75) b.B++; else if (s.overallScore >= 60) b.C++; else if (s.overallScore >= 40) b.D++; else b.F++; });

        if (distChart) distChart.destroy();
        distChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['A (≥90)', 'B (75–89)', 'C (60–74)', 'D (40–59)', 'F (<40)'],
                datasets: [{ data: Object.values(b), backgroundColor: [C.healthy, C.primary, C.warn, C.orange, C.danger], borderWidth: 0, borderRadius: 4, barPercentage: .6 }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { ticks: { color: C.text2, font: { family: 'Outfit', size: 11 } }, grid: { display: false } },
                    y: { beginAtZero: true, ticks: { color: C.text3, stepSize: 1, font: { family: 'Outfit', size: 10 } }, grid: { color: C.grid } }
                }
            }
        });
    }

    function renderRadar() {
        const ctx = document.getElementById('radarChart');
        if (!ctx || !scores.length) return;
        const avg = { q: mean(scores.map(s => s.breakdown?.quality || 0)), g: mean(scores.map(s => s.breakdown?.governance || 0)), l: mean(scores.map(s => s.breakdown?.lineage || 0)), f: mean(scores.map(s => s.breakdown?.freshness || 0)) };

        if (radar) radar.destroy();
        radar = new Chart(ctx, {
            type: 'radar',
            data: {
                labels: ['Quality', 'Governance', 'Lineage', 'Freshness'],
                datasets: [{ data: [avg.q, avg.g, avg.l, avg.f], backgroundColor: C.primaryBg, borderColor: C.primary, borderWidth: 2, pointBackgroundColor: C.primary, pointBorderColor: '#fff', pointBorderWidth: 2, pointRadius: 4 }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { r: { beginAtZero: true, max: 100, ticks: { stepSize: 25, color: C.text3, backdropColor: 'transparent', font: { size: 9 } }, grid: { color: C.grid }, angleLines: { color: C.grid }, pointLabels: { color: C.text, font: { size: 11, weight: '600', family: 'Outfit' } } } }
            }
        });
    }

    /* ---- Time Machine ---- */
    window.__tm = async function (fqn, name) {
        const card = document.getElementById('tmCard');
        document.getElementById('tmName').textContent = '— ' + name;
        card.classList.add('open');
        card.scrollIntoView({ behavior: 'smooth', block: 'start' });
        try {
            const h = await get(`/api/scores/${encodeURIComponent(fqn)}/history?limit=50`);
            drawTm(h.reverse(), name);
        } catch { document.getElementById('tmName').textContent = '— failed to load'; }
    };

    function closeTm() { document.getElementById('tmCard').classList.remove('open'); if (tmChart) tmChart.destroy(); }

    function drawTm(h) {
        const ctx = document.getElementById('tmChart');
        const labels = h.map(x => { const d = new Date(x.computedAt); return d.toLocaleString('en', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }); });
        const ds = [
            { label: 'Overall', data: h.map(x => x.overallScore), borderColor: C.primary, borderWidth: 2.5, tension: .3, fill: false },
            { label: 'Quality', data: h.map(x => x.breakdown?.quality || 0), borderColor: C.healthy, borderWidth: 1.5, borderDash: [4, 4], tension: .3, fill: false },
            { label: 'Governance', data: h.map(x => x.breakdown?.governance || 0), borderColor: C.warn, borderWidth: 1.5, borderDash: [4, 4], tension: .3, fill: false },
            { label: 'Lineage', data: h.map(x => x.breakdown?.lineage || 0), borderColor: '#7C3AED', borderWidth: 1.5, borderDash: [4, 4], tension: .3, fill: false },
            { label: 'Freshness', data: h.map(x => x.breakdown?.freshness || 0), borderColor: C.danger, borderWidth: 1.5, borderDash: [4, 4], tension: .3, fill: false }
        ];
        if (tmChart) tmChart.destroy();
        tmChart = new Chart(ctx, {
            type: 'line', data: { labels, datasets: ds },
            options: {
                responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
                plugins: { legend: { labels: { color: C.text, usePointStyle: true, pointStyle: 'line', font: { size: 10, family: 'Outfit' } } } },
                scales: {
                    x: { ticks: { color: C.text3, maxRotation: 45, font: { size: 9 } }, grid: { display: false } },
                    y: { min: 0, max: 100, ticks: { color: C.text3, stepSize: 25, font: { size: 9 } }, grid: { color: C.grid } }
                }
            }
        });
    }

    /* ---- Asset Detail Panel ---- */
    const detailPanel = document.getElementById('detailPanel');
    const detailOverlay = document.getElementById('detailOverlay');
    
    function closeDetail() {
        detailPanel?.classList.remove('open');
        detailOverlay?.classList.remove('open');
    }
    
    document.getElementById('dpClose')?.addEventListener('click', closeDetail);
    detailOverlay?.addEventListener('click', closeDetail);

    async function openDetail(fqn) {
        if (!fqn) return;
        
        // Open the panel immediately with loading state
        detailPanel.classList.add('open');
        detailOverlay.classList.add('open');
        setText('dpTitle', 'Loading…');
        setText('dpFqn', fqn);
        setText('dpScore', '—');
        
        try {
            const d = await get(`/api/scores/${encodeURIComponent(fqn)}/detail`);
            
            // Title & FQN
            setText('dpTitle', d.displayName || d.fqn);
            setText('dpFqn', d.fqn);
            
            // Score & Grade
            const scoreEl = document.getElementById('dpScore');
            scoreEl.textContent = d.overallScore.toFixed(1);
            scoreEl.style.color = col(d.overallScore);
            
            const gradeEl = document.getElementById('dpGrade');
            gradeEl.textContent = d.grade;
            gradeEl.className = `dp-grade-big grade-${d.grade}`;
            
            // Trend
            const trendEl = document.getElementById('dpTrend');
            if (d.trend) {
                const dir = d.trend.direction;
                const arrow = dir === 'improving' ? '↑' : dir === 'declining' ? '↓' : '→';
                const cls = dir === 'improving' ? 'up' : dir === 'declining' ? 'down' : 'flat';
                trendEl.className = `dp-trend ${cls}`;
                trendEl.textContent = `${arrow} ${Math.abs(d.trend.delta)} pts`;
            } else {
                trendEl.textContent = '';
            }
            
            // Signal Breakdown
            const sigContainer = document.getElementById('dpSignals');
            const bk = d.breakdown;
            const signals = [
                { label: 'Quality', value: bk.quality, detail: bk.qualityDetail, weight: 35 },
                { label: 'Governance', value: bk.governance, detail: bk.governanceDetail, weight: 25 },
                { label: 'Lineage', value: bk.lineage, detail: bk.lineageDetail, weight: 25 },
                { label: 'Freshness', value: bk.freshness, detail: bk.freshnessDetail, weight: 15 }
            ];
            sigContainer.innerHTML = signals.map(s => `
                <div class="dp-sig">
                    <span class="dp-sig-label">${s.label} <span style="font-size:.55rem;color:var(--text-3)">(${s.weight}%)</span></span>
                    <div class="dp-sig-bar"><div class="dp-sig-fill" style="width:${Math.round(s.value)}%;background:${col(s.value)}"></div></div>
                    <span class="dp-sig-val" style="color:${col(s.value)}">${Math.round(s.value)}</span>
                </div>
                ${s.detail ? `<div class="dp-sig-detail" style="padding-left:90px;margin-top:-2px;margin-bottom:4px">${esc(s.detail)}</div>` : ''}
            `).join('');
            
            // Governance Checklist
            const govContainer = document.getElementById('dpGovChecks');
            if (d.governanceChecks && d.governanceChecks.length > 0) {
                govContainer.innerHTML = d.governanceChecks.map(c => `
                    <div class="dp-check">
                        <div class="dp-check-icon ${c.passed ? 'pass' : 'fail'}">${c.passed ? '✓' : '✗'}</div>
                        <span class="dp-check-label ${c.passed ? '' : 'fail'}">${esc(c.label)}</span>
                    </div>
                `).join('');
            } else {
                govContainer.innerHTML = '<div style="font-size:.75rem;color:var(--text-3)">No governance data available</div>';
            }
            
            // Recommendations
            const recContainer = document.getElementById('dpRecs');
            if (d.recommendations && d.recommendations.length > 0) {
                recContainer.innerHTML = d.recommendations.map(r => `
                    <div class="dp-rec ${r.priority}">
                        <span class="dp-rec-text">${esc(r.action)}</span>
                        <span class="dp-rec-impact">${r.impact}</span>
                    </div>
                `).join('');
            } else {
                recContainer.innerHTML = '<div style="font-size:.75rem;color:var(--healthy);font-weight:600">✓ No recommendations — this asset is well-governed!</div>';
            }
            
            // OM Link
            document.getElementById('dpOmLink').href = d.omLink || '#';
            
            // Trend button
            document.getElementById('dpTrendBtn').onclick = () => {
                closeDetail();
                window.__tm(d.fqn, d.displayName || d.fqn);
            };
            
        } catch (e) {
            setText('dpTitle', 'Error loading details');
            console.error('Detail load failed:', e);
        }
    }
    
    // Expose for external use
    window.__openDetail = openDetail;

    /* ---- Helpers ---- */
    function col(v) { return v >= 70 ? C.healthy : v >= 40 ? C.warn : C.danger; }
    function mean(a) { return a.length ? a.reduce((s, v) => s + v, 0) / a.length : 0; }
    function setText(id, v) { const e = document.getElementById(id); if (e) e.textContent = v; }
    function esc(s) { return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
    function escA(s) { return (s || '').replace(/'/g, "\\'").replace(/"/g, '&quot;'); }
})();

