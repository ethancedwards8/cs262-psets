#!/usr/bin/env python3
"""Read a paxosn3 message trace from stdin/file and produce an HTML sequence diagram."""

import sys
import re
import html
import math
import bisect
from collections import defaultdict
from datetime import datetime


# --- Parsing ---

def parse_timestamp(s):
    """Parse timestamp with variable-length fractional seconds to float seconds."""
    dot = s.index('.')
    frac = s[dot + 1:]
    # Truncate to microseconds for strptime
    s = s[:dot + 1] + frac[:6]
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f").timestamp()


def parse_node(s):
    """Strip /role suffix to get base node name (e.g. 'R1/r' -> 'R1')."""
    return s.split('/')[0]


def parse_lines(lines):
    """Parse log lines into (kind, ts, src, dst, msg) events.

    kind is 'send' or 'recv'.
    For send lines: src=left, dst=right.
    For recv lines: src=right, dst=left  (log format: DST ← SRC MSG).
    """
    pat = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+):\s+'
        r'(\S+)\s+(→|←)\s+(\S+)\s+(.+)'
    )
    events = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        m = pat.match(line)
        if not m:
            continue
        ts_str, left, arrow, right, msg = m.groups()
        ts = parse_timestamp(ts_str)
        left_node = parse_node(left)
        right_node = parse_node(right)
        msg = msg.strip()

        if arrow == '→':
            events.append(('send', ts, left_node, right_node, msg))
        else:
            # recv: DST ← SRC msg  →  src=right_node, dst=left_node
            events.append(('recv', ts, left_node, right_node, msg))
    return events


UNLOGGED_NODES = {'clients'}


def match_messages(events):
    """Match send/receive pairs.

    Returns (messages, unmatched_sends).
    messages:  [(send_ts, recv_ts, src, dst, msg), ...]
    unmatched: [(send_ts, src, dst, msg), ...]

    For messages to/from UNLOGGED_NODES (e.g. 'clients'), only one side is
    logged, so we treat them as instantaneous (send_ts == recv_ts).
    """
    send_queues = defaultdict(list)
    messages = []
    unmatched = []

    for ev in events:
        kind = ev[0]
        if kind == 'send':
            _, ts, src, dst, msg = ev
            if dst in UNLOGGED_NODES or src in UNLOGGED_NODES:
                # Replica→client sends have no matching recv logged
                messages.append((ts, ts, src, dst, msg))
            else:
                send_queues[(src, dst, msg)].append(ts)
        else:
            # recv: (kind, ts, dst, src, msg)  — dst ← src
            _, ts, dst, src, msg = ev
            if src in UNLOGGED_NODES or dst in UNLOGGED_NODES:
                # Client→replica recvs have no matching send logged
                messages.append((ts, ts, src, dst, msg))
            else:
                key = (src, dst, msg)
                if send_queues[key]:
                    send_ts = send_queues[key].pop(0)
                    messages.append((send_ts, ts, src, dst, msg))
                # else: orphan recv — ignore

    for (src, dst, msg), times in send_queues.items():
        for t in times:
            unmatched.append((t, src, dst, msg))

    return messages, unmatched


# --- Message classification ---

def msg_type(msg):
    paren = msg.find('(')
    return msg[:paren] if paren >= 0 else msg.split()[0]


# Color palette
_TYPE_COLORS = {
    'PROPOSE':     '#7a6fff',   # purple-blue
    'ACK':         '#40b870',   # green
    'REDIRECTION': '#888888',   # gray
    'CAS':         '#e8a838',   # orange
    'CAS_A':       '#f5d080',   # light orange
    'PUT':         '#e06060',   # red-orange
    'PUT_A':       '#f09090',   # light red
    'REMOVE':      '#4ab8d0',   # teal
    'REMOVE_A':    '#90d8e8',   # light teal
    'GET':         '#d0a0f0',   # lavender
    'GET_A':       '#e8d0ff',   # light lavender
}
_DEFAULT_COLOR = '#aaaaaa'


def msg_color(msg):
    return _TYPE_COLORS.get(msg_type(msg), _DEFAULT_COLOR)


def msg_stroke_width(msg):
    mt = msg_type(msg)
    if mt in ('PROPOSE',):
        return 2.0
    if mt in ('CAS_A', 'PUT_A', 'REMOVE_A', 'GET_A'):
        return 1.2
    return 1.5


def msg_is_response(msg):
    mt = msg_type(msg)
    return mt.endswith('_A') or mt == 'REDIRECTION'


# --- Time mapping (adapted from consensusvis.py) ---

MAX_RECV_PER_BAND = 4
RECV_BAND_PX = 5

SPLIT_ARROW_HEIGHT = 100
SPLIT_H_GAP = 50
SPLIT_CIRCLE_R = 3
SPLIT_RECV_ANGLE = 30
SPLIT_RECV_SLOPE = math.tan(math.radians(SPLIT_RECV_ANGLE))


def build_time_mapping(messages, unmatched, px_per_sec=200,
                       compress_threshold=0.25, compress_px=50,
                       min_arrow_px=20):
    all_times = set()
    for send_t, recv_t, src, dst, msg in messages:
        all_times.add(send_t)
        all_times.add(recv_t)
    for send_t, src, dst, msg in unmatched:
        all_times.add(send_t)
    times = sorted(all_times)
    n = len(times)

    if n <= 1:
        t0 = times[0] if times else 0.0
        return (lambda t: 0.0), 0.0, t0

    time_to_idx = {t: i for i, t in enumerate(times)}

    n_gaps = n - 1
    spacings = [0.0] * n_gaps
    for i in range(n_gaps):
        gap = times[i + 1] - times[i]
        spacings[i] = compress_px if gap >= compress_threshold else gap * px_per_sec

    prefix = [0.0] * n
    for i in range(1, n):
        prefix[i] = prefix[i - 1] + spacings[i - 1]

    scale = [1.0] * n_gaps
    for send_t, recv_t, src, dst, msg in messages:
        si = time_to_idx[send_t]
        ei = time_to_idx[recv_t]
        if si >= ei:
            continue
        span = prefix[ei] - prefix[si]
        if 0 < span < min_arrow_px:
            factor = min_arrow_px / span
            for j in range(si, ei):
                scale[j] = max(scale[j], factor)

    node_recvs = defaultdict(list)
    for send_t, recv_t, src, dst, msg in messages:
        node_recvs[dst].append(recv_t)
    window = MAX_RECV_PER_BAND + 1
    for nid, rtimes in node_recvs.items():
        rtimes.sort()
        for i in range(len(rtimes) - window + 1):
            si = time_to_idx[rtimes[i]]
            ei = time_to_idx[rtimes[i + window - 1]]
            if si >= ei:
                continue
            span = prefix[ei] - prefix[si]
            if 0 < span < RECV_BAND_PX:
                factor = RECV_BAND_PX / span
                for j in range(si, ei):
                    scale[j] = max(scale[j], factor)

    for i in range(n_gaps):
        spacings[i] *= scale[i]

    y_pos = [0.0] * n
    for i in range(1, n):
        y_pos[i] = y_pos[i - 1] + spacings[i - 1]

    total_height = y_pos[-1]
    y_dict = {t: y_pos[i] for i, t in enumerate(times)}

    def t_to_y_offset(t):
        if t in y_dict:
            return y_dict[t]
        if t <= times[0]:
            return y_pos[0]
        if t >= times[-1]:
            return y_pos[-1]
        idx = bisect.bisect_right(times, t) - 1
        if idx >= n - 1:
            return y_pos[-1]
        t0, t1 = times[idx], times[idx + 1]
        y0, y1 = y_pos[idx], y_pos[idx + 1]
        if t1 == t0:
            return y0
        return y0 + (t - t0) / (t1 - t0) * (y1 - y0)

    return t_to_y_offset, total_height, times[0]


# --- HTML generation ---

def generate_html(messages, unmatched, nodes):
    if not messages and not unmatched:
        return "<html><body>No messages.</body></html>"

    t_to_y_offset, total_y_height, t_min = build_time_mapping(messages, unmatched)

    node_list = sorted(nodes, key=lambda n: (n == 'clients', n))
    n_nodes = len(node_list)
    node_index = {n: i for i, n in enumerate(node_list)}

    col_width = 180
    left_margin = 100
    right_margin = 60
    top_margin = 60
    bottom_margin = 40
    svg_width = left_margin + n_nodes * col_width + right_margin
    svg_height = int(top_margin + total_y_height + bottom_margin)

    def t_to_y(t):
        return top_margin + t_to_y_offset(t)

    def node_x(node_id):
        return left_margin + node_index[node_id] * col_width + col_width // 2

    def _extra(split_id):
        return f' data-split="{split_id}"' if split_id is not None else ''

    def draw_line(parts, xa, ya, xb, yb, color, sw, tip, split_id=None):
        ex = _extra(split_id)
        parts.append(
            f'<line x1="{xa:.1f}" y1="{ya:.2f}" x2="{xb:.1f}" y2="{yb:.2f}" '
            f'stroke="{color}" stroke-width="{sw}" class="msg" data-tip="{tip}"{ex} />')

    def draw_arrowhead(parts, xa, ya, xb, yb, color, mt, tip, split_id=None):
        adx = xb - xa
        ady = yb - ya
        length = (adx * adx + ady * ady) ** 0.5
        if length > 0:
            ex = _extra(split_id)
            ux, uy = adx / length, ady / length
            px, py = -uy, ux
            ah, aw = (10, 4) if mt == 'PROPOSE' else (7, 3)
            ax = xb - ux * ah + px * aw
            ay = yb - uy * ah + py * aw
            bx = xb - ux * ah - px * aw
            by = yb - uy * ah - py * aw
            parts.append(
                f'<polygon points="{xb:.1f},{yb:.2f} {ax:.1f},{ay:.2f} {bx:.1f},{by:.2f}" '
                f'fill="{color}" class="msg" data-tip="{tip}"{ex} />')

    def draw_circle(parts, cx, cy, color, tip, split_id=None):
        ex = _extra(split_id)
        parts.append(
            f'<circle cx="{cx:.1f}" cy="{cy:.2f}" r="{SPLIT_CIRCLE_R}" '
            f'fill="#1a1a1a" stroke="{color}" stroke-width="1.5" '
            f'class="msg" data-tip="{tip}"{ex} />')

    parts = []

    # Node labels
    for nid in node_list:
        x = node_x(nid)
        label = nid
        parts.append(f'<text x="{x}" y="20" text-anchor="middle" '
                     f'font-size="14" font-weight="bold" fill="#ddd">{label}</text>')

    # Lifelines
    for nid in node_list:
        x = node_x(nid)
        parts.append(f'<line x1="{x}" y1="{top_margin - 10}" x2="{x}" '
                     f'y2="{svg_height - bottom_margin + 10}" '
                     f'stroke="#444" stroke-width="1" />')

    # Time ticks
    all_ts = sorted(set(
        [m[0] for m in messages] + [m[1] for m in messages] + [u[0] for u in unmatched]
    ))
    t_max = max(all_ts) if all_ts else t_min
    tick_start = int(t_min)
    if tick_start < t_min:
        tick_start += 1
    for sec in range(tick_start, int(t_max) + 2):
        t = float(sec)
        if t < t_min or t > t_max + 0.5:
            continue
        y = t_to_y(t)
        label = f'+{t - t_min:.0f}s'
        parts.append(f'<line x1="0" y1="{y:.1f}" x2="{svg_width}" y2="{y:.1f}" '
                     f'stroke="#2a2a2a" stroke-width="0.5" />')
        parts.append(f'<text x="8" y="{y + 4:.1f}" font-size="11" fill="#888">{label}</text>')

    # Message arrows
    split_counter = 0
    for send_t, recv_t, src, dst, msg in messages:
        x1 = node_x(src)
        y1 = t_to_y(send_t)
        x2 = node_x(dst)
        y2 = t_to_y(recv_t)
        color = msg_color(msg)
        sw = msg_stroke_width(msg)
        mt = msg_type(msg)
        latency_ms = (recv_t - send_t) * 1e3
        tip = html.escape(f'{msg}  {src}→{dst}  Δ{latency_ms:.1f}ms', quote=True)

        dy = y2 - y1
        dx = x2 - x1
        should_split = (dy > 4 * SPLIT_ARROW_HEIGHT
                        and (recv_t - send_t) >= 3.0
                        and abs(dx) >= SPLIT_H_GAP)

        if should_split:
            sid = split_counter
            split_counter += 1
            slope = SPLIT_ARROW_HEIGHT / dx
            sign_dx = 1 if dx > 0 else -1
            x_break = x2 - sign_dx * SPLIT_H_GAP
            y_break_send = y1 + (x_break - x1) * slope
            y_break_recv = y2 - SPLIT_H_GAP * SPLIT_RECV_SLOPE
            draw_line(parts, x1, y1, x_break, y_break_send, color, sw, tip, sid)
            draw_circle(parts, x_break, y_break_send, color, tip, sid)
            draw_circle(parts, x_break, y_break_recv, color, tip, sid)
            draw_line(parts, x_break, y_break_recv, x2, y2, color, sw, tip, sid)
            draw_arrowhead(parts, x_break, y_break_recv, x2, y2, color, mt, tip, sid)
        else:
            draw_line(parts, x1, y1, x2, y2, color, sw, tip)
            draw_arrowhead(parts, x1, y1, x2, y2, color, mt, tip)

    # Dropped sends — dashed stub with X
    for send_t, src, dst, msg in unmatched:
        x1 = node_x(src)
        y1 = t_to_y(send_t)
        x2 = node_x(dst)
        y2 = y1 + 15
        xmid = (x1 + x2) / 2
        ymid = (y1 + y2) / 2
        color = msg_color(msg)
        esc = html.escape(f'{msg}  {src}→{dst}  DROPPED', quote=True)
        parts.append(
            f'<line x1="{x1}" y1="{y1:.2f}" x2="{xmid:.1f}" y2="{ymid:.2f}" '
            f'stroke="{color}" stroke-width="1.5" stroke-dasharray="4,3" '
            f'class="msg" data-tip="{esc}" />'
        )
        sz = 3
        parts.append(
            f'<line x1="{xmid-sz:.1f}" y1="{ymid-sz:.2f}" '
            f'x2="{xmid+sz:.1f}" y2="{ymid+sz:.2f}" stroke="{color}" stroke-width="2" />'
        )
        parts.append(
            f'<line x1="{xmid+sz:.1f}" y1="{ymid-sz:.2f}" '
            f'x2="{xmid-sz:.1f}" y2="{ymid+sz:.2f}" stroke="{color}" stroke-width="2" />'
        )

    svg_body = '\n'.join(parts)
    n_dropped = len(unmatched)
    n_total = len(messages)

    # Build legend entries for types that appear in this trace
    seen_types = sorted({msg_type(m[4]) for m in messages} | {msg_type(u[3]) for u in unmatched})
    legend_items = ''
    for mt in seen_types:
        c = _TYPE_COLORS.get(mt, _DEFAULT_COLOR)
        legend_items += (
            f'<span><span class="swatch" style="background:{c}"></span>'
            f' {html.escape(mt)}</span>\n        '
        )

    page = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Paxos Sequence Diagram</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    background: #1a1a1a;
    color: #ddd;
    font-family: "Helvetica Neue", Arial, sans-serif;
    overflow: hidden;
    height: 100vh;
    display: flex;
    flex-direction: column;
}}
#toolbar {{
    background: #252525;
    padding: 8px 16px;
    display: flex;
    align-items: center;
    gap: 16px;
    border-bottom: 1px solid #333;
    flex-shrink: 0;
    flex-wrap: wrap;
}}
#toolbar .stats {{ font-size: 13px; color: #aaa; }}
#toolbar label {{ font-size: 13px; color: #ccc; }}
#toolbar input[type=range] {{ width: 120px; vertical-align: middle; }}
.legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    font-size: 12px;
}}
.legend span {{
    display: flex;
    align-items: center;
    gap: 4px;
}}
.legend .swatch {{
    display: inline-block;
    width: 20px;
    height: 3px;
    border-radius: 1px;
}}
#container {{
    flex: 1;
    overflow: auto;
    position: relative;
}}
#tooltip {{
    display: none;
    position: fixed;
    background: #333;
    color: #eee;
    padding: 5px 10px;
    border-radius: 4px;
    font-size: 13px;
    font-family: monospace;
    pointer-events: none;
    z-index: 100;
    white-space: nowrap;
    box-shadow: 0 2px 8px rgba(0,0,0,0.5);
}}
svg .msg {{
    cursor: pointer;
    transition: opacity 0.1s;
}}
svg .msg:hover, svg .msg.split-hl {{
    opacity: 1 !important;
    filter: brightness(1.4);
}}
</style>
</head>
<body>
<div id="toolbar">
    <div class="stats">{n_total} messages, {n_dropped} dropped</div>
    <label>Zoom: <input type="range" id="zoom" min="0.1" max="5" step="0.1" value="1">
    <span id="zoom-val">1.0x</span></label>
    <div class="legend">
        {legend_items}
    </div>
</div>
<div id="container">
    <svg id="diagram" xmlns="http://www.w3.org/2000/svg"
         width="{svg_width}" height="{svg_height}"
         viewBox="0 0 {svg_width} {svg_height}"
         style="transform-origin: top left;">
        <rect width="100%" height="100%" fill="#1a1a1a" />
        {svg_body}
    </svg>
</div>
<div id="tooltip"></div>
<script>
const svg = document.getElementById('diagram');
const tooltip = document.getElementById('tooltip');
const zoomSlider = document.getElementById('zoom');
const zoomVal = document.getElementById('zoom-val');
const baseW = {svg_width};
const baseH = {svg_height};

zoomSlider.addEventListener('input', () => {{
    const z = parseFloat(zoomSlider.value);
    zoomVal.textContent = z.toFixed(1) + 'x';
    svg.style.transform = `scale(${{z}})`;
    svg.style.width = (baseW * z) + 'px';
    svg.style.height = (baseH * z) + 'px';
}});

let curSplit = null;
svg.addEventListener('mousemove', (e) => {{
    const el = e.target;
    if (el.classList.contains('msg') && el.dataset.tip) {{
        tooltip.textContent = el.dataset.tip;
        tooltip.style.display = 'block';
        tooltip.style.left = (e.clientX + 12) + 'px';
        tooltip.style.top = (e.clientY - 28) + 'px';
        const sid = el.dataset.split;
        if (sid !== undefined && sid !== curSplit) {{
            if (curSplit !== null)
                svg.querySelectorAll('[data-split="'+curSplit+'"]').forEach(
                    n => n.classList.remove('split-hl'));
            svg.querySelectorAll('[data-split="'+sid+'"]').forEach(
                n => n.classList.add('split-hl'));
            curSplit = sid;
        }} else if (sid === undefined && curSplit !== null) {{
            svg.querySelectorAll('[data-split="'+curSplit+'"]').forEach(
                n => n.classList.remove('split-hl'));
            curSplit = null;
        }}
    }} else {{
        tooltip.style.display = 'none';
        if (curSplit !== null) {{
            svg.querySelectorAll('[data-split="'+curSplit+'"]').forEach(
                n => n.classList.remove('split-hl'));
            curSplit = null;
        }}
    }}
}});
svg.addEventListener('mouseleave', () => {{
    tooltip.style.display = 'none';
    if (curSplit !== null) {{
        svg.querySelectorAll('[data-split="'+curSplit+'"]').forEach(
            n => n.classList.remove('split-hl'));
        curSplit = null;
    }}
}});
</script>
</body>
</html>"""
    return page


# --- CLI ---

def usage(f=sys.stderr):
    print("""\
Usage: python3 paxosvis.py [OPTIONS] [TRACEFILE] > output.html

Read a paxosn3 message trace and produce an HTML sequence diagram.
Reads from TRACEFILE if given, otherwise stdin.

Options:
  --start T       Start T seconds after the first timestamp (default: 0)
  --end T         End T seconds after the first timestamp (default: all)
  --types TYPE,…  Comma-separated message types to include (default: all)
                  Example: --types PROPOSE,ACK
  --no-clients    Exclude client↔replica messages (keep only inter-replica)
  --no-dropped    Exclude dropped (unmatched) messages from the diagram
  -h, --help      Show this help

Message types: PROPOSE, ACK, CAS, CAS_A, PUT, PUT_A, REMOVE, REMOVE_A, REDIRECTION

Examples:
  # First 5 seconds of paxos messages only
  python3 paxosvis.py --end 5 --no-clients paxosn3.log > out.html

  # Just PROPOSE and ACK, 10-15 second window
  python3 paxosvis.py --types PROPOSE,ACK --start 10 --end 15 paxosn3.log > out.html
""", file=f)


def main():
    filename = None
    start_offset = None
    end_offset = None
    filter_types = None
    no_clients = False
    no_dropped = False

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ('-h', '--help'):
            usage(sys.stdout)
            sys.exit(0)
        elif arg == '--start':
            i += 1
            start_offset = float(args[i])
        elif arg == '--end':
            i += 1
            end_offset = float(args[i])
        elif arg == '--types':
            i += 1
            filter_types = set(args[i].split(','))
        elif arg == '--no-clients':
            no_clients = True
        elif arg == '--no-dropped':
            no_dropped = True
        elif arg.startswith('-'):
            print(f'Unknown option: {arg}', file=sys.stderr)
            usage()
            sys.exit(1)
        elif filename is None:
            filename = arg
        else:
            print('Too many arguments', file=sys.stderr)
            usage()
            sys.exit(1)
        i += 1

    if filename is not None:
        with open(filename) as f:
            lines = f.read().splitlines()
    elif sys.stdin.isatty():
        usage()
        sys.exit(1)
    else:
        lines = sys.stdin.read().splitlines()

    events = parse_lines(lines)
    if not events:
        print('No events parsed.', file=sys.stderr)
        sys.exit(1)

    t_first = min(ev[1] for ev in events)
    print(f'Parsed {len(events)} events, t_first={t_first:.3f}', file=sys.stderr)

    # Apply time filtering
    if start_offset is not None or end_offset is not None:
        t_start = t_first + (start_offset or 0.0)
        t_end   = t_first + end_offset if end_offset is not None else float('inf')
        events = [ev for ev in events if t_start <= ev[1] <= t_end]
        print(f'After time filter [{start_offset},{end_offset}]s: {len(events)} events',
              file=sys.stderr)

    # Collect nodes before message-type filtering (so all nodes show up as columns)
    nodes = set()
    for ev in events:
        if ev[0] == 'send':
            nodes.add(ev[2])
            nodes.add(ev[3])
        else:
            nodes.add(ev[2])
            nodes.add(ev[3])

    # Apply message-type filter
    client_nodes = {'clients'}
    if no_clients:
        events = [ev for ev in events
                  if ev[2] not in client_nodes and ev[3] not in client_nodes]
    if filter_types:
        events = [ev for ev in events if msg_type(ev[4]) in filter_types]

    if not events:
        print('No events remain after filtering.', file=sys.stderr)
        sys.exit(1)

    messages, unmatched = match_messages(events)
    if no_dropped:
        unmatched = []

    print(f'{len(messages)} messages, {len(unmatched)} dropped', file=sys.stderr)

    if len(messages) > 5000:
        print(f'Warning: {len(messages)} messages may produce a large SVG. '
              f'Consider --start/--end/--types to narrow the window.', file=sys.stderr)

    page = generate_html(messages, unmatched, nodes)
    sys.stdout.write(page)


if __name__ == '__main__':
    main()
