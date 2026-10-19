#!/usr/bin/env python3
"""T6: what a lease holder's own rules see of a VM pause whose clocks stand
still, modelled with the design's parameters (no VM needed).

The holder asks its grantor for a grant every heartbeat h on its own clock;
the grantor answers at once and keeps each grant for L of real time. The
holder counts the lease from when it asked (s) and ends it at
L * (1 - lease_drift - excess) on its own clock, where excess is how far the
rate bound (g_b - g_a) / (s_b - r_a), over the grants with the shortest round
trip in the first and last thirds of rate_window, exceeds 1 + lease_drift / 2
("Lease time" in docs/design.md). At real time tp the holder's VM stops for P
seconds and every holder clock stands still (QEMU stop/cont with kvmclock,
snapshot restore, migration downtime: T1-T4). A grant that arrives during the
pause is either queued and read on resume (stop/cont: the tap device holds it)
or lost (migration and restore: it went to the old host).

Stale exposure is the real time after resume during which the holder believes
it holds a lease that its grantor no longer keeps. A new primary can accept
writes only inside it, so it bounds how long stale reads are possible.
Blocked time is the real time after resume during which the grantor keeps a
lease the holder will not use: the cost of a rule in lost reads.
The holder rules, each adding to the design's rate bound:

  rate    the design's rate bound alone
  step    plus a step check on each grant: if g_b - g_a > (r_b - s_a)(1 + d/2),
          the holder lost time between the two grants and ends every lease
  hb      the step check on every message from the grantor that carries its
          clock, including the heartbeats it sends every h on its own; the
          holder handles a pass's peer messages before its reads
  marker  the rate bound plus the VMClock disruption marker, read each pass:
          it changes before the guest runs again, but only for snapshot
          restore and migration (T2-T4), whose in-flight grants are lost
  all     hb and marker together

On a detection (step or marker) the holder asks for a grant at once and
restarts its rate window, so the pause does not also count as a fast grantor.
"""
import heapq
import platform
import random
import statistics
import time

L = 4.0
D = 0.01
H = 1.0
W = 10.0
THRESH = 1 + D / 2
STEP = 0.001


def run(P, phase, queued, rule, seed):
    """Returns (stale exposure, blocked time, detection delay or None)."""
    rng = random.Random(seed)
    tp = 30.0 + phase
    resume = tp + P
    horizon = resume + 20

    def hclock(t):
        return t if t < tp else (tp if t < resume else t - P)

    def real_of(h):
        return h if h <= tp else h + P

    # Events in real time: ("send", due_on_holder_clock) and ("grant", s, g).
    heap = []
    seq = 0

    def push(t, ev):
        nonlocal seq
        heapq.heappush(heap, (t, seq, ev))
        seq += 1

    def send(t_real):
        rtt = rng.uniform(0.0002, 0.001)
        g = t_real + rtt / 2
        arrive = t_real + rtt
        if tp <= arrive < resume:
            if not queued:
                return
            arrive = resume
        push(arrive, ("grant", hclock(t_real), g))

    push(0.0005, ("tick", 0.0005))
    hb_phase = rng.uniform(0, H)
    if rule in ("hb", "all"):
        k = 0
        while k * H + hb_phase < horizon:
            t_send = k * H + hb_phase
            arrive = t_send + rng.uniform(0.0001, 0.0005)
            if tp <= arrive < resume:
                arrive = resume if queued else None
            if arrive is not None:
                push(arrive, ("hb", t_send))
            k += 1
    if rule in ("marker", "all") and not queued:
        push(resume, ("marker",))
    seen = []
    all_grants = []
    leases = []  # (real time from, asked at on holder clock, valid until on it)
    floors = [(0.0, -1.0)]  # (real time, leases asked for before this are ended)
    detected_at = None
    while heap:
        t, _, ev = heapq.heappop(heap)
        if t > horizon:
            break
        if ev[0] == "tick":
            send(t)
            push(real_of(ev[1] + H), ("tick", ev[1] + H))
        elif ev[0] == "hb":
            g, r = ev[1], hclock(t)
            a = seen[-1] if seen else None
            if a is not None and a[0] >= floors[-1][1] and g - a[1] > (r - a[0]) * THRESH:
                if detected_at is None and t >= resume:
                    detected_at = t
                floors.append((t, r))
                seen = []
                send(t)
        elif ev[0] == "marker":
            detected_at = resume
            floors.append((t, hclock(t)))
            seen = []
            send(t)
        else:
            _, s, g = ev
            r = hclock(t)
            prev = seen[-1] if seen else None
            seen.append((s, g, r))
            all_grants.append((s, g, r))
            excess = 0.0
            first = [x for x in seen if r - W <= x[2] < r - 2 * W / 3]
            last = [x for x in seen if r - W / 3 <= x[2] <= r]
            if first and last:
                a = min(first, key=lambda x: x[2] - x[0])
                b = min(last, key=lambda x: x[2] - x[0])
                if b[0] > a[2]:
                    excess = max(0.0, (b[1] - a[1]) / (b[0] - a[2]) - THRESH)
                    if excess > 0 and detected_at is None and t >= resume:
                        detected_at = t
            if (rule in ("step", "hb", "all") and prev is not None and s >= floors[-1][1]
                    and g - prev[1] > (r - prev[0]) * THRESH):
                if detected_at is None:
                    detected_at = t
                floors.append((t, r))
                seen = []
                send(t)
                continue
            leases.append((t, s, s + L * (1 - D - excess)))

    h0 = hclock(resume)
    leases = [x for x in leases if x[2] > h0]
    grants = sorted(g for _, g, _ in all_grants if g + L > resume - L)
    exposure = blocked = 0.0
    t = resume
    while t < horizon:
        h = hclock(t)
        floor = max(f for ft, f in floors if ft <= t)
        holder_valid = any(ft <= t and s >= floor and h < vu for ft, s, vu in leases)
        grantor_end = max((g + L for g in grants if g <= t), default=0.0)
        if holder_valid and t > grantor_end:
            exposure += STEP
        if not holder_valid and t <= grantor_end:
            blocked += STEP
        t += STEP
    return exposure, blocked, (detected_at - resume) if detected_at is not None else None


PHASES = 100
PS = (0.5, 2.0, 3.5, 5.0, 10.0, 30.0)
RULES = ("rate", "step", "hb", "marker", "all")


def table(rule, queued, P):
    ex, bl, det, missed = [], [], [], 0
    for i in range(PHASES):
        e, b, d = run(P, i / PHASES * H, queued, rule, seed=i)
        ex.append(e)
        bl.append(b)
        if d is None:
            missed += 1
        else:
            det.append(d)
    return ex, bl, det, missed


def main():
    print(f"# Python {platform.python_version()} | no VM | "
          f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    print(f"# T6 model: L={L:.0f} s, lease_drift={D:.0%}, heartbeat {H:.0f} s, rate_window "
          f"{W:.0f} s, round trips 0.2-1 ms; {PHASES} pause phases per row")
    print("# rule    in-flight  P(s)   stale exposure max/p50 (s)  blocked max/p50 (s)  "
          "detected after resume max (s)  undetected")
    worst = {}
    for queued in (True, False):
        for rule in RULES:
            for P in PS:
                ex, bl, det, missed = table(rule, queued, P)
                worst[(rule, queued, P)] = (max(ex), max(bl))
                print(f"R {rule:6s}  {'queued' if queued else 'lost  '}  {P:5.2f}  "
                      f"{max(ex):6.3f} / {statistics.median(ex):6.3f}       "
                      f"{max(bl):6.3f} / {statistics.median(bl):6.3f}    "
                      f"{(max(det) if det else float('nan')):7.3f}       "
                      f"{missed:3d}/{PHASES}", flush=True)

    def w(rule, queued=None, i=0):
        return max(v[i] for (r, q, P), v in worst.items()
                   if r == rule and (queued is None or q == queued))

    for rule, queued, want in (("rate", None, ">0"), ("step", None, ">0"),
                               ("hb", True, "0"), ("hb", False, ">0"),
                               ("marker", True, ">0"), ("marker", False, "0"),
                               ("all", None, "0")):
        got = w(rule, queued)
        ok = got > 0 if want == ">0" else got == 0
        what = {None: "any", True: "queued", False: "lost"}[queued]
        print(f"{'[OK ]' if ok else '[FAIL]'} T6 rule {rule} grants in flight {what}: worst "
              f"stale exposure got={got:.3f}s want={want}")
    got = max(w(r) for r in RULES)
    print(f"{'[OK ]' if got <= H + 0.01 else '[FAIL]'} T6 every rule: worst stale exposure "
          f"got={got:.3f}s want<=heartbeat {H:.0f}s")
    got = w("rate", None, 1)
    print(f"{'[OK ]' if got > 1 else '[FAIL]'} T6 rule rate: the pause shortens later leases "
          f"(worst blocked) got={got:.3f}s want>1s")


if __name__ == "__main__":
    main()
