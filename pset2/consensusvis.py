#!/usr/bin/env python3
"""Read a consensus message trace from stdin and produce an HTML sequence diagram."""

import sys
import re
import html
import math
import bisect
from collections import defaultdict
from datetime import datetime

def parse_timestamp(s):
    """Parse timestamp string to seconds as float."""
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
    return dt.timestamp()

def parse_lines(lines):
    """Parse message trace lines into events."""
    # Pattern: timestamp: src → dst "msg"  or  timestamp: dst ← src "msg"
    pat = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6})\d*:\s+'
        r'(-?\d+)\s+(→|←)\s+(-?\d+)\s+"([^"]*)"'
    )
    # Pattern for receiver-only format: timestamp: dst ← "msg" (no sender)
    pat_recv_nosrc = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6})\d*:\s+'
        r'(-?\d+)\s+←\s+"([^"]*)"'
    )
    events = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("***"):
            continue
        m = pat.search(line)
        if m:
            ts = parse_timestamp(m.group(1))
            left = int(m.group(2))
            arrow = m.group(3)
            right = int(m.group(4))
            msg = m.group(5)
            if arrow == "→":
                events.append(("send", ts, left, right, msg))
            else:  # ←
                events.append(("recv", ts, left, right, msg))
            continue
        m = pat_recv_nosrc.search(line)
        if m:
            ts = parse_timestamp(m.group(1))
            dst = int(m.group(2))
            msg = m.group(3)
            events.append(("recv_nosrc", ts, dst, msg))
    return events

def match_messages(events):
    """Match send/receive pairs into messages. Returns (messages, unmatched_sends)."""
    # Queue sends by (src, dst, msg_text)
    send_queues = defaultdict(list)
    messages = []       # (send_time, recv_time, src, dst, msg)
    unmatched = []      # (send_time, src, dst, msg) — no receive

    for ev in events:
        if ev[0] == "send":
            _, ts, src, dst, msg = ev
            send_queues[(src, dst, msg)].append(ts)
        elif ev[0] == "recv":
            _, ts, dst, src, msg = ev  # recv line: dst ← src
            key = (src, dst, msg)
            if send_queues[key]:
                send_ts = send_queues[key].pop(0)
                messages.append((send_ts, ts, src, dst, msg))
            # else: orphan receive — ignore
        else:  # recv_nosrc — receive without sender, assign sequentially
            _, ts, dst, msg = ev
            # Find the send queue with the earliest timestamp for (*, dst, msg)
            best_key = None
            best_ts = None
            for key, times in send_queues.items():
                if key[1] == dst and key[2] == msg and times:
                    if best_ts is None or times[0] < best_ts:
                        best_ts = times[0]
                        best_key = key
            if best_key is not None:
                send_ts = send_queues[best_key].pop(0)
                messages.append((send_ts, ts, best_key[0], dst, msg))
            # else: orphan receive — ignore

    # Remaining sends are unmatched (dropped messages)
    for (src, dst, msg), times in send_queues.items():
        for t in times:
            unmatched.append((t, src, dst, msg))

    return messages, unmatched

def msg_type(msg):
    """Extract message type from message string like 'PREPARE(1, red, 0)'."""
    paren = msg.find("(")
    return msg[:paren] if paren >= 0 else msg

VALUE_COLORS = {
    "red":   "#ff4444",
    "blue":  "#4488ff",
}

def msg_value_color(msg):
    """Extract the consensus value (red/blue) from a message and return its bright color."""
    for val, color in VALUE_COLORS.items():
        if val in msg:
            return color
    return None

def msg_color(msg):
    mt = msg_type(msg)
    if mt == "PREPARE":
        return "#aa55dd"
    elif mt == "PROPOSE":
        return "#e8a838"
    elif mt == "ACK":
        return "#50b050" if "true" in msg else "#888888"
    elif mt == "DECIDE":
        return msg_value_color(msg) or "#d94a4a"
    return "#888888"

def msg_stroke_width(msg):
    return 3.0 if msg_type(msg) == "DECIDE" else 1.5


def build_time_mapping(messages, unmatched, px_per_sec=200,
                       compress_threshold=0.25, compress_px=50,
                       min_arrow_px=20):
    """Build a non-linear time-to-Y mapping.

    Returns (t_to_y_offset, total_height, t_min).
    """
    # Collect all timestamps
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

    # Initial gap spacings
    n_gaps = n - 1
    spacings = [0.0] * n_gaps
    for i in range(n_gaps):
        gap = times[i + 1] - times[i]
        if gap >= compress_threshold:
            spacings[i] = compress_px
        else:
            spacings[i] = gap * px_per_sec

    # Build initial prefix sums for min-arrow-height check
    prefix = [0.0] * n
    for i in range(1, n):
        prefix[i] = prefix[i - 1] + spacings[i - 1]

    # Enforce min arrow height: compute per-gap scale factor
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

    # Enforce receive density: every (MAX_RECV_PER_BAND + 1) consecutive
    # receives on a node must span ≥ RECV_BAND_PX
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

    # Apply scale factors
    for i in range(n_gaps):
        spacings[i] *= scale[i]

    # Build final Y positions
    y_pos = [0.0] * n
    for i in range(1, n):
        y_pos[i] = y_pos[i - 1] + spacings[i - 1]

    total_height = y_pos[-1]

    # Dict for fast lookup of known timestamps
    y_dict = {t: y_pos[i] for i, t in enumerate(times)}

    # Interpolation function for arbitrary timestamps
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
        frac = (t - t0) / (t1 - t0)
        return y0 + frac * (y1 - y0)

    return t_to_y_offset, total_height, times[0]


# --- Split-arrow constants ---
SPLIT_ARROW_HEIGHT = 100   # virtual vertical extent of a split arrow (px)
SPLIT_H_GAP = 50           # horizontal distance from destination for break (px)
SPLIT_CIRCLE_R = 3         # radius of open circles at break points
SPLIT_RECV_ANGLE = 30       # receiver-side slope angle in degrees from horizontal
SPLIT_RECV_SLOPE = math.tan(math.radians(SPLIT_RECV_ANGLE))  # dy/dx for receiver side

# --- Receive-density constants ---
MAX_RECV_PER_BAND = 4      # max receives per node in a RECV_BAND_PX window
RECV_BAND_PX = 5           # vertical window size for density limit


def generate_html(messages, unmatched, nodes):
    """Generate a self-contained HTML page with the sequence diagram."""
    if not messages and not unmatched:
        return "<html><body>No messages.</body></html>"

    # Build non-linear time mapping
    px_per_sec = 200
    t_to_y_offset, total_y_height, t_min = \
        build_time_mapping(messages, unmatched, px_per_sec=px_per_sec)

    # Layout constants
    node_list = sorted(nodes)
    n_nodes = len(node_list)
    node_index = {n: i for i, n in enumerate(node_list)}

    col_width = 160
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

    # --- Helper: extra attributes string for split-arrow grouping ---
    def _extra(split_id):
        return f' data-split="{split_id}"' if split_id is not None else ''

    # --- Helper: draw a line segment with optional dotted value-color overlay ---
    def draw_line(parts, xa, ya, xb, yb, color, sw, mt, msg, tip, split_id=None):
        ex = _extra(split_id)
        parts.append(
            f'<line x1="{xa:.1f}" y1="{ya:.2f}" x2="{xb:.1f}" y2="{yb:.2f}" '
            f'stroke="{color}" stroke-width="{sw}" class="msg" data-tip="{tip}"{ex} />')
        if mt in ("PREPARE", "PROPOSE"):
            vc = msg_value_color(msg)
            if vc:
                parts.append(
                    f'<line x1="{xa:.1f}" y1="{ya:.2f}" x2="{xb:.1f}" y2="{yb:.2f}" '
                    f'stroke="{vc}" stroke-width="4" stroke-dasharray="3,18" '
                    f'class="msg" data-tip="{tip}"{ex} />')

    # --- Helper: draw an arrowhead pointing from (xa,ya) toward (xb,yb) ---
    def draw_arrowhead(parts, xa, ya, xb, yb, color, mt, tip, split_id=None):
        adx = xb - xa
        ady = yb - ya
        length = (adx*adx + ady*ady) ** 0.5
        if length > 0:
            ex = _extra(split_id)
            ux, uy = adx/length, ady/length
            px, py = -uy, ux
            ah = 8 if mt != "DECIDE" else 11
            aw = 3.5 if mt != "DECIDE" else 5
            ax = xb - ux * ah + px * aw
            ay = yb - uy * ah + py * aw
            bx = xb - ux * ah - px * aw
            by = yb - uy * ah - py * aw
            parts.append(
                f'<polygon points="{xb:.1f},{yb:.2f} {ax:.1f},{ay:.2f} {bx:.1f},{by:.2f}" '
                f'fill="{color}" class="msg" data-tip="{tip}"{ex} />')

    # --- Helper: open circle ---
    def draw_circle(parts, cx, cy, color, tip, split_id=None):
        ex = _extra(split_id)
        parts.append(
            f'<circle cx="{cx:.1f}" cy="{cy:.2f}" r="{SPLIT_CIRCLE_R}" '
            f'fill="#1a1a1a" stroke="{color}" stroke-width="1.5" '
            f'class="msg" data-tip="{tip}"{ex} />')

    # Build SVG pieces
    parts = []

    # Header / node labels
    for nid in node_list:
        x = node_x(nid)
        label = f"Server {nid}" if nid >= 0 else "Nancy"
        parts.append(f'<text x="{x}" y="20" text-anchor="middle" '
                     f'font-size="14" font-weight="bold" fill="#ddd">{label}</text>')

    # Vertical lifelines
    for nid in node_list:
        x = node_x(nid)
        parts.append(f'<line x1="{x}" y1="{top_margin - 10}" x2="{x}" y2="{svg_height - bottom_margin + 10}" '
                     f'stroke="#444" stroke-width="1" />')

    # Time axis ticks — every second
    all_times_list = sorted(
        set([m[0] for m in messages] + [m[1] for m in messages] +
            [u[0] for u in unmatched]))
    t_max = max(all_times_list)
    tick_start = int(t_min)
    if tick_start < t_min:
        tick_start += 1
    for sec in range(tick_start, int(t_max) + 2):
        t = float(sec)
        if t < t_min or t > t_max + 0.5:
            continue
        y = t_to_y(t)
        rel = t - t_min
        label = f"+{rel:.0f}s"
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
        tip = html.escape(f"{msg}  {src}→{dst}  Δ{latency_ms:.0f}ms", quote=True)

        dy = y2 - y1
        dx = x2 - x1
        should_split = (dy > 4 * SPLIT_ARROW_HEIGHT
                        and (recv_t - send_t) >= 3.0
                        and abs(dx) >= SPLIT_H_GAP)

        if should_split:
            sid = split_counter
            split_counter += 1

            # Virtual arrow slope: as if the arrow traversed SPLIT_ARROW_HEIGHT
            slope = SPLIT_ARROW_HEIGHT / dx   # dy/dx for the virtual arrow
            sign_dx = 1 if dx > 0 else -1

            # Break point: SPLIT_H_GAP from destination, back toward source
            x_break = x2 - sign_dx * SPLIT_H_GAP

            # Sending part: (x1, y1) → (x_break, y_break_send)
            y_break_send = y1 + (x_break - x1) * slope

            # Receiving part: (x_break, y_break_recv) → (x2, y2), fixed 30° slope
            y_break_recv = y2 - SPLIT_H_GAP * SPLIT_RECV_SLOPE

            # Draw sending part
            draw_line(parts, x1, y1, x_break, y_break_send, color, sw, mt, msg, tip, sid)
            draw_circle(parts, x_break, y_break_send, color, tip, sid)

            # Draw receiving part
            draw_circle(parts, x_break, y_break_recv, color, tip, sid)
            draw_line(parts, x_break, y_break_recv, x2, y2, color, sw, mt, msg, tip, sid)
            draw_arrowhead(parts, x_break, y_break_recv, x2, y2, color, mt, tip, sid)
        else:
            # Normal (unsplit) arrow
            draw_line(parts, x1, y1, x2, y2, color, sw, mt, msg, tip)
            draw_arrowhead(parts, x1, y1, x2, y2, color, mt, tip)

    # Unmatched sends — dashed line ending partway
    for send_t, src, dst, msg in unmatched:
        x1 = node_x(src)
        y1 = t_to_y(send_t)
        x2 = node_x(dst)
        # End partway (at y1 + a small offset) with an X
        y2 = y1 + 15
        xmid = (x1 + x2) / 2
        ymid = (y1 + y2) / 2
        color = msg_color(msg)
        esc = html.escape(f"{msg}  {src}→{dst}  DROPPED", quote=True)
        parts.append(
            f'<line x1="{x1}" y1="{y1:.2f}" x2="{xmid:.1f}" y2="{ymid:.2f}" '
            f'stroke="{color}" stroke-width="1.5" stroke-dasharray="4,3" '
            f'class="msg" data-tip="{esc}" />'
        )
        # X mark at end
        sz = 3
        parts.append(
            f'<line x1="{xmid-sz:.1f}" y1="{ymid-sz:.2f}" x2="{xmid+sz:.1f}" y2="{ymid+sz:.2f}" '
            f'stroke="{color}" stroke-width="2" />'
        )
        parts.append(
            f'<line x1="{xmid+sz:.1f}" y1="{ymid-sz:.2f}" x2="{xmid-sz:.1f}" y2="{ymid+sz:.2f}" '
            f'stroke="{color}" stroke-width="2" />'
        )

    svg_body = "\n".join(parts)
    n_dropped = len(unmatched)
    n_total = len(messages)

    page = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Message Sequence Diagram</title>
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
#toolbar .stats {{
    font-size: 13px;
    color: #aaa;
}}
#toolbar label {{
    font-size: 13px;
    color: #ccc;
}}
#toolbar input[type=range] {{
    width: 120px;
    vertical-align: middle;
}}
.legend {{
    display: flex;
    gap: 14px;
    font-size: 13px;
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
    stroke-width: 3 !important;
    filter: brightness(1.3);
}}
</style>
</head>
<body>
<div id="toolbar">
    <div class="stats">{n_total} messages, {n_dropped} dropped</div>
    <label>Zoom: <input type="range" id="zoom" min="0.1" max="5" step="0.1" value="1">
    <span id="zoom-val">1.0x</span></label>
    <div class="legend">
        <span><span class="swatch" style="background:#aa55dd"></span> PREPARE</span>
        <span><span class="swatch" style="background:#e8a838"></span> PROPOSE</span>
        <span><span class="swatch" style="background:#50b050"></span> ACK(true)</span>
        <span><span class="swatch" style="background:#888"></span> ACK(false)</span>
        <span><span class="swatch" style="background:#ff4444; height:5px"></span> DECIDE(red)</span>
        <span><span class="swatch" style="background:#4488ff; height:5px"></span> DECIDE(blue)</span>
        <span style="margin-left:8px; color:#888">dotted overlay = value color</span>
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
const container = document.getElementById('container');
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


def usage(file):
    print("Usage: python3 consensusvis.py [TRACEFILE] > OUTPUT.html\n"
          "\n"
          "Read a consensus message trace (produced by `ctconsensus -V`) and produce an HTML\n"
          "sequence diagram on stdout. Reads from TRACEFILE if given, otherwise stdin.\n"
          "\nFormat examples:\n"
          "[send]           2021-10-12 20:21:09.000000: 2 → 1 \"PREPARE(1, blue, 0)\"\n"
          "[receive]        2021-10-12 20:21:09.020005: 1 ← \"PREPARE(1, blue, 0)\"\n"
          "[recv w/sender]  2021-10-12 20:21:09.020005: 1 ← 2 \"PREPARE(1, blue, 0)\"",
          file=file)

def main():
    filename = None
    for arg in sys.argv[1:]:
        if arg == "-h" or arg == "--help":
            usage(sys.stderr)
            sys.exit(0)
        elif arg.startswith("-"):
            print(f"Unknown option: {arg}", file=sys.stderr)
            usage(sys.stderr)
            sys.exit(1)
        elif filename is None:
            filename = arg
        else:
            print(f"Too many arguments", file=sys.stderr)
            usage(sys.stderr)
            sys.exit(1)

    if filename is not None:
        with open(filename) as f:
            lines = f.read().splitlines()
    elif sys.stdin.isatty():
        usage(sys.stderr)
        sys.exit(1)
    else:
        lines = sys.stdin.read().splitlines()
    events = parse_lines(lines)
    if not events:
        print("No events parsed.", file=sys.stderr)
        sys.exit(1)

    # Collect all node IDs
    nodes = set()
    for ev in events:
        if ev[0] == "send":
            nodes.add(ev[2])
            nodes.add(ev[3])
        elif ev[0] == "recv":
            nodes.add(ev[2])
            nodes.add(ev[3])
        else:  # recv_nosrc
            nodes.add(ev[2])

    messages, unmatched = match_messages(events)
    print(f"Parsed {len(events)} events → {len(messages)} messages, "
          f"{len(unmatched)} dropped", file=sys.stderr)

    page = generate_html(messages, unmatched, nodes)
    sys.stdout.write(page)


if __name__ == "__main__":
    main()
