#!/usr/bin/env python3
"""Checks lease-margin rules against adversarial clocks (studies/lease-margins).

Real time is in seconds. Each node has two clocks: raw (CLOCK_MONOTONIC_RAW),
running at its boot calibration's rate, and boot (CLOCK_BOOTTIME), running at
raw times (1 + slew), where a time daemon may slew by up to 22.6% and "no
daemon" means slew 0. The holder asks at real time t, ends the lease when
either of its clocks shows L(1 - m) since then; the grantor reads its clocks
when it grants and keeps the grant until both show L. Overlap is how long the
holder's lease outlasts the grant (positive is unsafe).

Part A: the margin m as a function of the rate bound B, per rule variant.
Part B: random and corner cases, B measured from grant round trips as the
        design says (shortest round trip of the window's first and last thirds).
Part C: a pair over 60 s with the step check, a partition at every heartbeat.
Part D: pause detection with the step threshold tied to the margin.
Part E: a grantor reboots inside the holder's rate window, window keyed or not.
Part F: the margin of the first lease after a start or detection.

Run: python3 -I margins.py > results.txt   (stdlib only, deterministic)
"""
import itertools
import random

L = 4.0
LD = 0.01
H = 1.0
WINDOW = 10.0
SLEW = 0.226
RATE_GAP = 0.1
MAX_MARGIN = 0.5


class Clock:
    """Piecewise-constant rate; segs is [(start, rate), ...] from t = 0."""

    def __init__(self, segs):
        self.segs = segs

    def read(self, t):
        v = 0.0
        for i, (t0, rate) in enumerate(self.segs):
            t1 = self.segs[i + 1][0] if i + 1 < len(self.segs) else float("inf")
            if t <= t0:
                break
            v += (min(t, t1) - t0) * rate
        return v

    def when(self, t, x):
        """Earliest real time at which the clock has advanced x since t."""
        target = self.read(t) + x
        v = 0.0
        for i, (t0, rate) in enumerate(self.segs):
            t1 = self.segs[i + 1][0] if i + 1 < len(self.segs) else float("inf")
            seg = (t1 - t0) * rate
            if v + seg >= target:
                return t0 + (target - v) / rate
            v += seg
        raise ValueError


def m_design(b):
    return LD + max(0.0, b - (1 + LD / 2))


def m_spike(b):
    return LD + (b - 1) if b > 1 + LD / 2 else LD


def m_excess_over_ld(b):
    return LD + max(0.0, b - (1 + LD))


def m_excess_over_2ld(b):
    return LD + max(0.0, b - (1 + 2 * LD))


def m_exact(b):
    return max(LD, 1 - 1 / b)


def m_exact_nofloor(b):
    return max(0.0, 1 - 1 / b)


def m_none(b):
    return LD


RULES = [
    ("design", m_design),            # ld + excess of B over 1 + ld/2
    ("spike README", m_spike),       # ld + (B - 1) once B > 1 + ld/2
    ("excess>1+ld", m_excess_over_ld),
    ("excess>1+2ld *", m_excess_over_2ld),
    ("max(ld,1-1/B)", m_exact),
    ("1-1/B, no floor *", m_exact_nofloor),
    ("ld alone", m_none),            # the first rate_window today
]


def part_a():
    print("== A. Margin each rule gives, against the least margin that is safe")
    print("   B bounds the grantor's raw rate over the holder's. Worst case: holder's")
    print("   boot clock no faster than its raw, grantor's no slower, so only raw counts:")
    print("   safe iff (1 - m) <= 1/rho, i.e. m >= 1 - 1/rho.")
    print(f"   {'rho':>7} {'needed':>8} " + " ".join(f"{n[:13]:>13}" for n, _ in RULES[:5]))
    for rho in (0.99, 1.0, 1.004, 1.005, 1.006, 1.01, 1.0101, 1.02, 1.04, 1.10):
        need = max(0.0, 1 - 1 / rho)
        cells = []
        for _, f in RULES[:5]:
            m = f(rho)
            cells.append(f"{m*100:11.3f}%{'!' if m < need - 1e-12 else ' '}")
        print(f"   {rho:7.4f} {need*100:7.3f}% " + " ".join(cells))
    print("   (! = less than needed. For B = 1.006, the design gives 1.1%, the spike README 1.6%,")
    print("    0.596% is needed, and lease_drift alone (1%) already covers it.)")
    print("   Headroom = margin given - margin needed: how far rho may rise after B was measured.")
    for name, f in RULES:
        worst = min(f(1 + i / 10000) - (1 - 1 / (1 + i / 10000)) for i in range(0, 2001))
        print(f"   {name:18s} least headroom over rho 1..1.2: {worst*100:+.4f}% of the lease")
    print()


def node_clocks(raw_rate, slew, step_at=None, step=0.0):
    raw = [(0.0, raw_rate)] if step_at is None else [(0.0, raw_rate), (step_at, raw_rate * (1 + step))]
    boot = [(t0, r * (1 + slew)) for t0, r in raw]
    return Clock(raw), Clock(boot)


def measure_bound(h_raw, g_raw, rng, rtts, fracs):
    """Grants every H for a whole window; returns B from the thirds."""
    samples = []
    for k, t in enumerate(range(int(WINDOW / H) + 1)):
        t = float(t)
        rtt = rtts(k)
        f = fracs(k)
        s, r, g = h_raw.read(t), h_raw.read(t + rtt), g_raw.read(t + f * rtt)
        samples.append((t, s, r, g))
    first = [x for x in samples if x[0] <= WINDOW / 3]
    last = [x for x in samples if x[0] >= 2 * WINDOW / 3]
    a = min(first, key=lambda x: x[2] - x[1])
    b = min(last, key=lambda x: x[2] - x[1])
    return (b[3] - a[3]) / (b[1] - a[2])


def lease_overlap(h, g, m, t_req, grant_delay):
    h_raw, h_boot = h
    g_raw, g_boot = g
    h_end = min(c.when(t_req, L * (1 - m)) for c in (h_raw, h_boot))
    t_g = t_req + grant_delay
    g_end = max(c.when(t_g, L) for c in (g_raw, g_boot))
    return h_end - g_end


def part_b():
    print("== B. Worst overlap per rule, B measured from grant round trips")
    print("   E = each node's raw calibration error bound; slew per node within +-22.6% or none;")
    print("   round trips 50 us to 1 ms, grantor's read anywhere inside; corners plus 20,000 random.")
    print("   'step' = a raw rate step on both nodes after the window (holder slower, grantor")
    print("   faster), as a clocksource change would make; it is outside what B measured.")
    rng = random.Random(1)
    cases = [("E 1%", 0.01, 0.0), ("E 4% (Skylake-X crystal bug)", 0.04, 0.0),
             ("E 1%, step 0.4% after window", 0.01, 0.004), ("E 1%, step 1% after window", 0.01, 0.01)]
    print(f"   {'case':32s} " + " ".join(f"{n[:14]:>14}" for n, _ in RULES))
    print("   (* = a planted variant the check must fail)")
    for label, e, step in cases:
        worst = {n: -1e9 for n, _ in RULES}
        trials = []
        for hr, gr in itertools.product((1 - e, 1, 1 + e), repeat=2):
            for hs, gs in itertools.product((-SLEW, 0.0, SLEW), repeat=2):
                trials.append((hr, gr, hs, gs, "tight"))
        for _ in range(20000):
            trials.append((1 + rng.uniform(-e, e), 1 + rng.uniform(-e, e),
                           rng.choice((0.0, rng.uniform(-SLEW, SLEW))),
                           rng.choice((0.0, rng.uniform(-SLEW, SLEW))), "random"))
        for hr, gr, hs, gs, kind in trials:
            h = node_clocks(hr, hs, WINDOW + 0.5 if step else None, -step / 2)
            g = node_clocks(gr, gs, WINDOW + 0.5 if step else None, step / 2)
            if kind == "tight":
                # g_a read as late as possible, g_b as early: B equals the true ratio.
                b = measure_bound(h[0], g[0], rng, lambda k: 50e-6, lambda k: 1.0 if k < 4 else 0.0)
                delay = 0.0
            else:
                b = measure_bound(h[0], g[0], rng, lambda k: rng.uniform(50e-6, 1e-3), lambda k: rng.random())
                delay = rng.uniform(0, 1e-3)
            for name, f in RULES:
                o = lease_overlap(h, g, f(b), WINDOW + 1.0, delay)
                worst[name] = max(worst[name], o)
        print(f"   {label:32s} " + " ".join(f"{worst[n]*1000:+11.3f} ms" for n, _ in RULES))
    print("   (positive = the holder's lease outlasts the grant; 'ld alone' is the first rate_window)")
    print("   design = ld + excess of B over 1 + ld/2; spike README = ld + (B - 1) once B > 1 + ld/2.")
    print()


def pair_run(rho_g, rule, t_part, rtt=100e-6):
    """Holder raw exact, grantor raw rho_g, both boot clocks equal to raw (no
    daemon). The holder asks every heartbeat, again at once after a detection,
    and, under a rule with a probe, again `probe` after the first grant of a
    window arrives, so that s_b - r_a >= probe. The network partitions at t_part. Returns (trips, first usable
    lease, margin of the last lease, overlap of the last lease)."""
    h_raw = Clock([(0.0, 1.0)])
    g_raw = Clock([(0.0, rho_g)])
    pending = [k * H for k in range(int(t_part / H) + 1)]
    samples, last, trips, first_use, lease = [], None, 0, None, None
    while pending:
        pending.sort()
        t = pending.pop(0)
        if t + rtt > t_part:
            continue
        s, r, g = h_raw.read(t), h_raw.read(t + rtt), g_raw.read(t + rtt / 2)
        if last is not None:
            m_now = lease[2] if lease else None
            if (g - last[2]) > (r - last[0]) * rule["step"](m_now):
                trips += 1
                samples, last, lease = [], None, None
                pending.append(t + rtt)
                continue
        samples.append((s, r, g))
        last = (s, r, g)
        bound = None
        span = s - samples[0][1]
        if len(samples) > 1 and span >= rule["min_span"] - 1e-9:
            first = [x for x in samples if x[0] - samples[0][0] <= span / 3] or samples[:1]
            a = min(first, key=lambda x: x[1] - x[0])
            bound = (g - a[2]) / (s - a[1])
        if bound is not None:
            m = rule["margin"](bound)
            if m >= MAX_MARGIN:
                m = None
        else:
            m = LD if rule["lease_without_bound"] else None
            if rule.get("probe") and len(samples) == 1:
                pending.append(t + rtt + rule["probe"])
        if m is None:
            lease = None
            continue
        if first_use is None:
            first_use = t + rtt
        lease = (t, t + rtt / 2, m)
    if lease is None:
        return trips, first_use, None, None
    t_req, t_grant, m = lease
    return trips, first_use, m, h_raw.when(t_req, L * (1 - m)) - g_raw.when(t_grant, L)


def part_c():
    print("== C. A pair over its first 60 s: step check, rate window and partition")
    print("   Holder's clocks exact, grantor's both rho fast (no daemon), 100 us round trips,")
    print("   a grant per 1 s heartbeat. The network partitions at a random time in 0..60 s")
    print("   (2,000 times); worst overlap of the holder's last lease. trips = step-check")
    print("   detections in a 60 s run. 'first read' = when the holder first holds a lease.")
    covered = lambda m: float("inf") if m is None else 1 / (1 - m)
    design = {"min_span": WINDOW, "margin": m_design, "lease_without_bound": True,
              "step": lambda m: 1 + LD / 2}
    design_c = dict(design, step=covered)
    proposed = {"min_span": RATE_GAP, "margin": m_design, "lease_without_bound": False,
                "probe": RATE_GAP, "step": covered}
    next_hb = dict(proposed, min_span=H / 2, probe=None)
    planted = dict(proposed, step=lambda m: 1 + LD / 2)
    rng = random.Random(2)
    times = [rng.uniform(0.0, 60.0) for _ in range(2000)]
    for name, rule in (("design as written: step threshold 1 + ld/2, lease_drift alone until a full window", design),
                       ("step threshold 1/(1-m) for the margin m in force, otherwise as written", design_c),
                       ("proposed: also no lease without a bound, bound from two grants >= 100 ms apart", proposed),
                       ("alternative: as proposed, but the bound from the next heartbeat's grant (1 s)", next_hb),
                       ("planted: as proposed, but step threshold 1 + ld/2 (fails liveness, not safety)", planted)):
        print(f"   {name}")
        print(f"   {'rho':>7} {'trips':>6} {'first read':>11} {'margin at 60 s':>15} {'worst overlap':>14}")
        for rho in (1.0, 1.004, 1.006, 1.0101, 1.012, 1.02, 1.04):
            worst = -1e9
            for tp in times:
                o = pair_run(rho, rule, tp)[3]
                if o is not None:
                    worst = max(worst, o)
            trips, first, m_late, _ = pair_run(rho, rule, 60.0)
            fr = f"{first*1000:8.1f} ms" if first is not None else "never"
            ml = f"{m_late*100:13.3f}%" if m_late is not None else "none"
            wo = f"{worst*1000:+11.2f} ms" if worst > -1e8 else "no lease"
            print(f"   {rho:7.4f} {trips:6d} {fr:>11} {ml:>15} {wo:>14}")
    print()


def part_d():
    print("== D. Pause detection with the step threshold 1/(1-m)")
    print("   Grantor raw rho, holder's exact, margin m = the design's rule at B = rho. The")
    print("   holder's clocks stand still for p between grant a and a heartbeat the grantor")
    print("   sends 1 s later (100 us round trips). Largest p the check misses, against the")
    print("   headroom the margin leaves on a 4 s lease (real time the holder may lose unseen).")
    rtt = 100e-6
    print(f"   {'rho':>7} {'m':>7} {'missed pause, threshold 1/(1-m)':>32} {'with 1+ld/2':>12} {'headroom':>9}")
    for rho in (1.0, 1.004, 1.006, 1.0101, 1.02, 1.04):
        m = m_design(rho)
        g_gap = rho * (1.0 - rtt / 2)

        def missed(thresh):
            # trip iff g_gap > thresh * (holder elapsed); holder elapsed = 1 + rtt/2 - p
            return max(0.0, 1.0 + rtt / 2 - g_gap / thresh)
        headroom = L / rho - L * (1 - m)
        p_c = missed(1 / (1 - m))
        p_d = missed(1 + LD / 2)
        print(f"   {rho:7.4f} {m*100:6.3f}% {p_c*1000:29.2f} ms {p_d*1000:9.2f} ms {headroom*1000:6.1f} ms")
    print("   A missed pause is always shorter than the headroom, so the lease still ends first;")
    print("   with 1+ld/2 a grantor faster than about 1.005 trips the check with no pause at all.")


def part_e():
    print("== E. A grantor reboots inside the holder's rate window")
    print("   Holder exact. The grantor's first boot runs raw at 1.0 from t = 0; it goes down at")
    print("   5.5 s and its second boot starts at 7.5 s with raw from 0 at rate 1.02. Grants every")
    print("   heartbeat while it is up, 100 us round trips; the network partitions just after the")
    print("   grant at 8 s (or after the probe, when there is one).")
    rtt = 1e-4
    boots = [(0.0, 5.5, 1.0), (7.5, 1e9, 1.02)]

    def g_read(t):
        for start, end, rate in boots:
            if start <= t < end:
                return (t - start) * rate, rate, start
        return None

    for keyed in (False, True):
        samples, boot_seen, lease = [], None, None
        events = [float(k) for k in range(0, 9)]
        while events:
            events.sort()
            t = events.pop(0)
            got = g_read(t + rtt / 2)
            if got is None:
                continue
            g, rate, boot = got
            s, r = t, t + rtt
            if keyed and boot != boot_seen:
                samples = []
            boot_seen = boot
            samples.append((s, r, g))
            m = None
            span = s - samples[0][1]
            if len(samples) > 1 and span >= RATE_GAP - 1e-9:
                first = [x for x in samples if x[0] - samples[0][0] <= span / 3] or samples[:1]
                a = min(first, key=lambda x: x[1] - x[0])
                b = (g - a[2]) / (s - a[1])
                m = m_design(b)
            elif len(samples) == 1:
                events.append(t + rtt + RATE_GAP)
            if m is not None:
                lease = (t, t + rtt / 2, m, rate, b)
            if t >= 8.0 and (not keyed or len(samples) > 1):
                break
        t_req, t_grant, m, rate, b = lease
        overlap = L * (1 - m) - (t_grant - t_req) - L / rate
        label = "window keyed by grantor boot" if keyed else "window not keyed (as specified)"
        print(f"   {label:32s} bound {b:+9.4f}  margin {m*100:5.2f}%  overlap {overlap*1000:+7.2f} ms")
    print("   Unkeyed, the first grant of the new boot gives a negative 'bound', the rule turns it")
    print("   into lease_drift, and the step check (g - g_a < 0) sees nothing.")
    print()


def part_f():
    print("== F. Margin of the first lease after a start or a detection")
    print("   Worst bound from two grants: B = rho (gap + 2 RTT) / gap, with the second request")
    print("   sent `gap` after the first grant arrives. Margin by the design's rule; no lease at")
    print(f"   {MAX_MARGIN:.0%} or more. Lease = how long the holder keeps a 4 s lease.")
    print(f"   {'gap':>6} {'rtt':>7} {'rho':>6} {'B':>8} {'margin':>8} {'lease':>8}")
    for gap in (RATE_GAP, H):
        for rtt in (100e-6, 1e-3, 10e-3):
            for rho in (1.0, 1.04):
                b = rho * (gap + 2 * rtt) / gap
                m = m_design(b)
                lease = f"{L*(1-m):6.3f} s" if m < MAX_MARGIN else "none"
                print(f"   {gap*1000:4.0f}ms {rtt*1e6:5.0f}us {rho:6.3f} {b:8.4f} {m*100:7.2f}% {lease:>8}")
    print("   The margin falls back towards lease_drift + (rho - 1.005) as the window lengthens.")
    print()


if __name__ == "__main__":
    print(f"# lease_drift {LD:.0%}, primary_lease {L:.0f} s, heartbeat {H:.0f} s, rate_window {WINDOW:.0f} s, slew +-{SLEW:.1%}")
    print()
    part_a()
    part_b()
    part_c()
    part_d()
    print()
    part_e()
    part_f()
