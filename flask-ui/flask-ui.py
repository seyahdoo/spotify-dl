"""Read-only web dashboard for the spotidl download worker.

Shows live progress and the tracks currently being downloaded. Configuration
is read from the environment (a ``secrets.env`` file is loaded when present).
"""

from __future__ import annotations

import logging
import os

import pymongo
from bson import json_util
from dotenv import load_dotenv
from flask import Flask, Response, render_template_string

load_dotenv("secrets.env")

LOGGER = logging.getLogger("spotidl.ui")

MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
MONGODB_DB_NAME = os.environ.get("MONGODB_DB_NAME", "db")
FLASK_HOST = os.environ.get("FLASK_HOST", "0.0.0.0")
FLASK_PORT = int(os.environ.get("FLASK_PORT", "8000"))
FLASK_DEBUG = os.environ.get("FLASK_DEBUG", "false").lower() in {"1", "true", "yes"}

# Must match STATUS_CONFIG in the dashboard template.
STATUS_KEYS = (
    "ready_to_start",
    "downloading",
    "errored",
    "not_found",
    "missing_in_disk",
    "found_in_disk",
    "done",
)

app = Flask(__name__)

pymongo_client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
pymongo_db = pymongo_client[MONGODB_DB_NAME]
songs_collection: pymongo.collection.Collection = pymongo_db.songs_collection

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Song Downloader Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-900 text-slate-100 min-h-screen p-6 md:p-10 font-sans">
    <div class="max-w-6xl mx-auto space-y-8">
        
        <!-- Header -->
        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4 border-b border-slate-800 pb-5">
            <div>
                <h1 class="text-2xl font-bold tracking-tight text-white">Music Sync Status</h1>
                <p class="text-slate-400 text-sm mt-1">Real-time MongoDB collection monitor</p>
            </div>
            <div id="status-badge" class="flex items-center gap-2 text-xs font-medium text-emerald-400 bg-emerald-950/60 border border-emerald-800/60 px-3 py-1.5 rounded-full self-start sm:self-auto">
                <span class="relative flex h-2 w-2">
                    <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                    <span class="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                </span>
                <span id="status-text">Live Sync</span>
            </div>
        </div>

        <!-- Progress & Pie Chart Section -->
        <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
            
            <!-- Left Column: Overall Progress Bars -->
            <div class="lg:col-span-5 bg-slate-800/40 border border-slate-700/60 rounded-xl p-6 flex flex-col justify-between space-y-6 shadow-lg">
                <div>
                    <div class="flex items-baseline justify-between">
                        <h2 class="text-base font-semibold text-white">Overall Progress</h2>
                        <span id="total-tracks-label" class="text-xs font-mono text-slate-400">0 total tracks</span>
                    </div>
                    <p class="text-xs text-slate-400 mt-1">Tracks processed vs. waiting in queue</p>
                </div>

                <!-- Main Progress Bar -->
                <div class="space-y-2">
                    <div class="flex justify-between items-end">
                        <span class="text-xs font-medium text-slate-300">Queue Completion</span>
                        <span id="progress-pct" class="text-2xl font-bold text-emerald-400">0.0%</span>
                    </div>
                    <div class="w-full h-4 bg-slate-900 rounded-full overflow-hidden border border-slate-700/60 p-0.5">
                        <div id="progress-bar" class="h-full bg-gradient-to-r from-blue-500 via-teal-400 to-emerald-400 rounded-full transition-all duration-500 ease-out" style="width: 0%"></div>
                    </div>
                    <div class="flex justify-between text-xs text-slate-400 pt-1">
                        <span id="processed-count-label">0 processed</span>
                        <span id="remaining-count-label">0 in queue</span>
                    </div>
                </div>

                <!-- Multi-Segment Stacked Bar -->
                <div class="space-y-2 pt-4 border-t border-slate-700/50">
                    <div class="flex justify-between text-xs">
                        <span class="text-slate-400">Health Breakdown</span>
                        <span id="success-rate-label" class="text-teal-400 font-medium">0% available</span>
                    </div>
                    <div class="w-full h-3 bg-slate-900 rounded-full overflow-hidden flex">
                        <div id="bar-success" class="bg-emerald-500 h-full transition-all duration-500" style="width: 0%" title="Done + On Disk"></div>
                        <div id="bar-active" class="bg-sky-400 h-full transition-all duration-500" style="width: 0%" title="Downloading + Ready"></div>
                        <div id="bar-issues" class="bg-rose-500 h-full transition-all duration-500" style="width: 0%" title="Errored + Not Found + Missing"></div>
                    </div>
                    <div class="flex items-center justify-between text-[11px] text-slate-400 pt-1">
                        <span class="flex items-center gap-1.5"><span class="w-2 h-2 rounded-full bg-emerald-500"></span>Available</span>
                        <span class="flex items-center gap-1.5"><span class="w-2 h-2 rounded-full bg-sky-400"></span>Queue/Active</span>
                        <span class="flex items-center gap-1.5"><span class="w-2 h-2 rounded-full bg-rose-500"></span>Issues</span>
                    </div>
                </div>
            </div>

            <!-- Right Column: Dependency-Free SVG Pie Chart & Counts -->
            <div class="lg:col-span-7 bg-slate-800/40 border border-slate-700/60 rounded-xl p-6 shadow-lg flex flex-col justify-between">
                <div class="mb-4">
                    <h2 class="text-base font-semibold text-white">Status Distribution</h2>
                    <p class="text-xs text-slate-400 mt-0.5">Live count breakdown across all collection states</p>
                </div>

                <div class="grid grid-cols-1 sm:grid-cols-12 gap-6 items-center my-auto">
                    <!-- Native SVG Doughnut Chart (No external JS library needed) -->
                    <div class="sm:col-span-5 relative flex items-center justify-center">
                        <svg id="pie-svg" viewBox="-1.2 -1.2 2.4 2.4" class="w-48 h-48 -rotate-90 overflow-visible"></svg>
                        <div class="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
                            <span id="pie-center-total" class="text-xl font-bold text-white font-mono">0</span>
                            <span class="text-[10px] uppercase tracking-wider text-slate-400">Total</span>
                        </div>
                    </div>

                    <!-- Incorporated Counts Legend -->
                    <div id="chart-legend" class="sm:col-span-7 grid grid-cols-1 sm:grid-cols-2 gap-2.5"></div>
                </div>
            </div>

        </div>

        <!-- Active Downloads Table -->
        <div class="bg-slate-800/40 border border-slate-700/60 rounded-xl overflow-hidden shadow-lg">
            <div class="px-6 py-4 border-b border-slate-700/60 flex items-center justify-between">
                <div class="flex items-center gap-2.5">
                    <h2 class="text-base font-semibold text-white">Currently Downloading</h2>
                    <span id="downloading-count" class="bg-blue-500/20 text-blue-300 text-xs font-semibold px-2.5 py-0.5 rounded-full border border-blue-500/30">
                        {{ downloading_songs|length }}
                    </span>
                </div>
            </div>

            <div id="table-wrapper" class="{{ 'hidden' if not downloading_songs else 'overflow-x-auto' }}">
                <table class="w-full text-left border-collapse">
                    <thead>
                        <tr class="border-b border-slate-700/60 text-xs uppercase tracking-wider text-slate-400 bg-slate-800/60">
                            <th class="py-3.5 px-6">#</th>
                            <th class="py-3.5 px-6">Track</th>
                            <th class="py-3.5 px-6">Artist</th>
                            <th class="py-3.5 px-6">Playlist</th>
                            <th class="py-3.5 px-6">Owner</th>
                            <th class="py-3.5 px-6">Track ID</th>
                        </tr>
                    </thead>
                    <tbody id="downloading-tbody" class="divide-y divide-slate-700/40 text-sm">
                        {% for song in downloading_songs %}
                        <tr class="hover:bg-slate-800/50 transition-colors">
                            <td class="py-3.5 px-6 text-slate-400 font-mono">{{ song.get('track_number', '-') }}</td>
                            <td class="py-3.5 px-6 font-medium text-white">{{ song.get('song_name', 'Unknown') }}</td>
                            <td class="py-3.5 px-6 text-slate-300">{{ song.get('artist_name', 'Unknown') }}</td>
                            <td class="py-3.5 px-6">
                                <span class="bg-slate-700/70 text-slate-200 px-2.5 py-1 rounded-md text-xs">
                                    {{ song.get('playlist_name', '-') }}
                                </span>
                            </td>
                            <td class="py-3.5 px-6 text-slate-400">{{ song.get('owner', '-') }}</td>
                            <td class="py-3.5 px-6 text-slate-500 font-mono text-xs">{{ song.get('track_id', '-') }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

            <div id="empty-state" class="{{ 'hidden' if downloading_songs else 'p-12 text-center text-slate-500' }}">
                <p class="text-base font-medium text-slate-400">No active downloads right now</p>
                <p class="text-xs mt-1">Tracks with status "downloading" will appear here automatically.</p>
            </div>
        </div>

    </div>

    {% raw %}
    <script>
        const STATUS_CONFIG = [
            { key: 'done',            label: 'Done',        color: '#10b981' },
            { key: 'found_in_disk',   label: 'On Disk',     color: '#14b8a6' },
            { key: 'downloading',     label: 'Downloading', color: '#38bdf8' },
            { key: 'ready_to_start',  label: 'Ready',       color: '#3b82f6' },
            { key: 'missing_in_disk', label: 'Missing',     color: '#f59e0b' },
            { key: 'not_found',       label: 'Not Found',   color: '#f97316' },
            { key: 'errored',         label: 'Errored',     color: '#f43f5e' }
        ];

        function escapeHtml(value) {
            return String(value ?? '-')
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#039;');
        }

        function renderSvgPie(stats, total) {
            const svg = document.getElementById('pie-svg');
            document.getElementById('pie-center-total').textContent = total.toLocaleString();

            if (total === 0) {
                svg.innerHTML = `<circle cx="0" cy="0" r="0.8" fill="none" stroke="#334155" stroke-width="0.35" />`;
                return;
            }

            let cumulativeFraction = 0;
            const r = 0.8;
            const circumference = 2 * Math.PI * r;
            let circlesHtml = '';

            STATUS_CONFIG.forEach(item => {
                const count = stats[item.key] || 0;
                if (count === 0) return;

                const fraction = count / total;
                const strokeLength = fraction * circumference;
                const offset = -cumulativeFraction * circumference;
                const pct = (fraction * 100).toFixed(1);

                circlesHtml += `
                    <circle
                        cx="0" cy="0" r="${r}"
                        fill="none"
                        stroke="${item.color}"
                        stroke-width="0.36"
                        stroke-dasharray="${strokeLength} ${circumference}"
                        stroke-dashoffset="${offset}"
                        class="transition-all duration-500 hover:opacity-80 cursor-pointer"
                    >
                        <title>${item.label}: ${count.toLocaleString()} (${pct}%)</title>
                    </circle>
                `;
                cumulativeFraction += fraction;
            });

            svg.innerHTML = circlesHtml;
        }

        function updateVisuals(stats) {
            const total = STATUS_CONFIG.reduce((sum, item) => sum + (stats[item.key] || 0), 0);
            const activeAndQueued = (stats.ready_to_start || 0) + (stats.downloading || 0);
            const processed = Math.max(0, total - activeAndQueued);
            const available = (stats.done || 0) + (stats.found_in_disk || 0);
            const issues = (stats.errored || 0) + (stats.not_found || 0) + (stats.missing_in_disk || 0);

            const completionPct = total > 0 ? ((processed / total) * 100) : 0;
            const availablePct = total > 0 ? ((available / total) * 100) : 0;
            const activePct = total > 0 ? ((activeAndQueued / total) * 100) : 0;
            const issuesPct = total > 0 ? ((issues / total) * 100) : 0;

            // 1. Update Progress Bars
            document.getElementById('total-tracks-label').textContent = `${total.toLocaleString()} total tracks`;
            document.getElementById('progress-pct').textContent = `${completionPct.toFixed(1)}%`;
            document.getElementById('progress-bar').style.width = `${completionPct}%`;
            document.getElementById('processed-count-label').textContent = `${processed.toLocaleString()} processed`;
            document.getElementById('remaining-count-label').textContent = `${activeAndQueued.toLocaleString()} in queue`;

            document.getElementById('success-rate-label').textContent = `${availablePct.toFixed(1)}% available`;
            document.getElementById('bar-success').style.width = `${availablePct}%`;
            document.getElementById('bar-active').style.width = `${activePct}%`;
            document.getElementById('bar-issues').style.width = `${issuesPct}%`;

            // 2. Render SVG Doughnut Chart
            renderSvgPie(stats, total);

            // 3. Render Custom Legend with Counts & Percentages
            const legendContainer = document.getElementById('chart-legend');
            legendContainer.innerHTML = STATUS_CONFIG.map(item => {
                const count = stats[item.key] || 0;
                const pct = total > 0 ? ((count / total) * 100).toFixed(1) : '0.0';
                return `
                    <div class="flex items-center justify-between bg-slate-900/60 border border-slate-700/50 rounded-lg px-3 py-2">
                        <div class="flex items-center gap-2 min-w-0">
                            <span class="w-2.5 h-2.5 rounded-full shrink-0" style="background-color: ${item.color}"></span>
                            <span class="text-xs text-slate-300 truncate">${item.label}</span>
                        </div>
                        <div class="flex items-baseline gap-1.5 shrink-0 ml-2">
                            <span class="text-sm font-bold text-white font-mono">${count.toLocaleString()}</span>
                            <span class="text-[10px] text-slate-500 font-mono">(${pct}%)</span>
                        </div>
                    </div>
                `;
            }).join('');
        }

        async function refreshDashboard() {
            try {
                const res = await fetch('/api/status');
                if (!res.ok) throw new Error('API request failed');
                const data = await res.json();

                updateVisuals(data.stats);

                const songs = data.downloading_songs;
                document.getElementById('downloading-count').textContent = songs.length;

                const tableWrapper = document.getElementById('table-wrapper');
                const emptyState = document.getElementById('empty-state');
                const tbody = document.getElementById('downloading-tbody');

                if (songs.length > 0) {
                    tableWrapper.className = 'overflow-x-auto';
                    emptyState.className = 'hidden';

                    tbody.innerHTML = songs.map(song => `
                        <tr class="hover:bg-slate-800/50 transition-colors">
                            <td class="py-3.5 px-6 text-slate-400 font-mono">${escapeHtml(song.track_number ?? '-')}</td>
                            <td class="py-3.5 px-6 font-medium text-white">${escapeHtml(song.song_name ?? 'Unknown')}</td>
                            <td class="py-3.5 px-6 text-slate-300">${escapeHtml(song.artist_name ?? 'Unknown')}</td>
                            <td class="py-3.5 px-6">
                                <span class="bg-slate-700/70 text-slate-200 px-2.5 py-1 rounded-md text-xs">
                                    ${escapeHtml(song.playlist_name ?? '-')}
                                </span>
                            </td>
                            <td class="py-3.5 px-6 text-slate-400">${escapeHtml(song.owner ?? '-')}</td>
                            <td class="py-3.5 px-6 text-slate-500 font-mono text-xs">${escapeHtml(song.track_id ?? '-')}</td>
                        </tr>
                    `).join('');
                } else {
                    tableWrapper.className = 'hidden';
                    emptyState.className = 'p-12 text-center text-slate-500';
                    tbody.innerHTML = '';
                }

                document.getElementById('status-text').textContent = 'Live Sync';
            } catch (err) {
                console.error('Background sync error:', err);
                document.getElementById('status-text').textContent = 'Reconnecting...';
            }
        }
    </script>
    {% endraw %}

    <script>
        // Inject initial server-side counts and start polling
        const initialStats = {{ stats | tojson }};
        updateVisuals(initialStats);
        setInterval(refreshDashboard, 1000);
    </script>
</body>
</html>
"""


def get_dashboard_data():
    try:
        stats = {
            status: songs_collection.count_documents({"status": status})
            for status in STATUS_KEYS
        }
        downloading_songs = list(songs_collection.find({"status": "downloading"}))
    except pymongo.errors.PyMongoError as exc:
        LOGGER.error("Failed to read dashboard data: %s", exc)
        stats = {status: 0 for status in STATUS_KEYS}
        downloading_songs = []
    return stats, downloading_songs


@app.route("/")
def index():
    stats, downloading_songs = get_dashboard_data()
    return render_template_string(
        HTML_TEMPLATE,
        stats=stats,
        downloading_songs=downloading_songs
    )


@app.route("/api/status")
def api_status():
    stats, downloading_songs = get_dashboard_data()
    return Response(
        json_util.dumps({"stats": stats, "downloading_songs": downloading_songs}),
        mimetype="application/json",
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG)