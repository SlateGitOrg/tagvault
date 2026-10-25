# Swinging-door trending and its error bounds

This note explains how tagvault reduces data, what it guarantees, and how each guarantee is
tested. Code: `src/compress/deadband.rs`, `src/compress/sdt.rs`. Tests: `tests/properties.rs`.

## The reconstruction being bounded

The archive stores a subset of the received points. A query reconstructs the signal by linear
interpolation between consecutive stored points (analog tags) or by holding the previous value
(discrete tags). See `src/query/mod.rs`. "Error" always means the vertical distance between a
received sample and this reconstruction at the sample's timestamp.

## Stage 1: exception deadband (collector)

The exception filter (the PI-interface style) reports a point when

* it differs from the last reported value by more than `exc_dev`, or
* its quality differs from the last reported quality, or
* `exc_max_ms` has elapsed since the last report.

When a deviation triggers a report, the previously received, unreported point is reported first.
That way the archive sees where the excursion started.

**Bound (hold).** For every received point `x`, let `r` be the last reported point at or before
`x.t`. Then `|x.v − r.v| ≤ exc_dev`. Proof: the only points that are not reported are points
within `exc_dev` of the last reported value.

**Bound (linear), 2·exc_dev.** Take two consecutive reported points `r₁` and `r₂`, and a point
`x` between them that was not reported. Every point in `(r₁, r₂)` is within `exc_dev` of `r₁.v`.
That holds for `r₂` as well, unless `r₂` is the point that broke the band. In that case the
point just before `r₂` was also reported, which contradicts the assumption that `x` lies between
consecutive reported points. So `r₁.v`, `r₂.v` and `x.v` all lie in `[r₁.v − e, r₁.v + e]`. Any
value on the segment `r₁ → r₂` lies in that interval too, so the error is at most `2e`.

## Stage 2: swinging door (archive)

Let `A` be the anchor (the last archived point). For a later point `Q`, the slopes from `A`
whose line passes within `d = comp_dev` of `Q` form the interval

```
win(Q) = [ (Q.v − d − A.v) / (Q.t − A.t) ,  (Q.v + d − A.v) / (Q.t − A.t) ]
```

These are the two "doors" hinged at `A ± d`. Write `W(S)` for the intersection of `win` over a
set of points `S`. Any slope in `W(S)` gives a line from `A` that passes within `d` of every
point in `S`. Each new point can only shrink `W`, which is why the doors "swing shut".

### Why textbook SDT can exceed its deviation

Classic SDT keeps the held point `H` while `W(all points since A) ≠ ∅`. When the doors cross,
it archives the measured `H`. The slope `slope(A → H)` lies in `win(H)`, but it does not have to
lie in the window of the earlier points. Counter-example (`d = 1`):

```
A = (0, 0)   Q = (1, 0)   H = (10, 10.5)   P = (11, 30)
win(Q) = [−1, 1]   win(H) = [0.95, 1.15]   →  W = [0.95, 1]  ≠ ∅, so H is held
P closes the doors → archive H.  Line A→H at t=1 is 1.05, but Q = 0 → error 1.05 > d
```

`classic_mode_can_exceed_deviation` in `sdt.rs` encodes this example. A mutation run (switching
the property test to classic mode) fails within a few cases. On the held-out benchmark signals,
classic SDT exceeded the bound on 2 of 16 temperature series and on 16 of 16 pressure series.

### tagvault's rules

| mode | rule | archived values | state | guarantee |
|---|---|---|---|---|
| **lookback** (default) | track the latest *feasible* point `F`, meaning `slope(A→F) ∈ W(points before F)`. When the doors cross at `P`, archive `F` and re-run the buffered points after `F` from the new anchor | measured | open segment (≤ 4 096 points) | `≤ d` |
| strict | the new `P` may replace `H` only if `slope(A→P) ∈ W(points before P)`. Otherwise archive `H` | measured | O(1) | `≤ d` |
| classic | textbook test, archive measured `H` | measured | O(1) | none (comparison only) |

**Proof for lookback.** `F` is archived only when the line `A → F` passes within `d` of every
point between `A` and `F`, because that is the definition of feasible. The points after `F` are
then re-run from the new anchor, so they are covered by later segments under the same rule. At
least one point is always feasible: the first point after `A` has no predecessors, so its
window is unconstrained. On a flush or a quality change, the open segment is closed by repeating
"archive the latest feasible point, re-run the rest" until the buffer is empty. Every received
good point therefore lies within `d` of the reconstruction, and every archived value is an
original measurement.

**Proof for strict.** Invariant: `slope(A → H) ∈ W(points strictly between A and H)`. It holds
trivially when `H` is the first point after `A`, and the acceptance test keeps it true. So
archiving `H` is always valid.

**Cost of the bound.** Strict mode ends a segment at the first infeasible new point. With noise
on a ramp, that happens often. Lookback continues as long as the doors are open, then falls
back to the latest point that is feasible. In practice that is almost always `H`, so it
archives about as few points as classic SDT. The held-out benchmark (`make bench`) measured
374.8x for lookback, 57.7x for strict and 610.6x for classic on tank level, and 259.5x,
44.4x and 335.2x on temperature.

An alternative was prototyped and dropped. "Fitted" mode used classic decisions but moved `H`
onto the closest feasible line. It also guarantees `≤ d`, but each anchor ended up at the edge of
the corridor, so the next segment started with less room. In an exploratory run it compressed
tank level about 2.5x worse than lookback. The prototype was not kept; see `DECISIONS.md`.

### Quality, discrete tags, timeouts

* Non-good points (Bad, or Stale after an NDEATH or DDEATH) close the open segment. Only quality
  transitions are archived, so a run of bad values becomes one point. Queries treat the signal
  as undefined from a non-good point until the next good point.
* Discrete (boolean) tags archive a point only when the value changes or `comp_max_ms` expires.
  With step reconstruction that is exact.
* `comp_max_ms` ends the open segment when the next point would be more than that long after
  the anchor.

## Combined bound

The collector filters with `e = exc_dev` and the archive compresses the reported points with
`d = comp_dev`. Every raw sample `x` then satisfies

```
|x.v − A(x.t)| ≤ 2e + d
```

Here `A` is the archive's linear reconstruction. Proof: the archived breakpoints are a subset of
the reported timestamps. So between two consecutive reported points, both `A` and the
reported-point interpolation `L` are straight lines, and their difference is linear on that
interval. At the endpoints the difference is at most `d` (the SDT bound), so it is at most `d`
everywhere between them. Add the `2e` bound between `L` and the raw samples.

The same argument bounds aggregates. Between consecutive raw samples, both the reconstruction
and the raw linear interpolation are straight lines. So `|TWA(archive) − TWA(raw)| ≤ 2e + d` over
any interval. The benchmark measured a worst case of 0.127× the bound over every 15-minute
interval of 64 held-out 6-hour series.

## How it is tested

| property | test |
|---|---|
| strict and lookback error ≤ `d` on random signals (irregular sampling, steps, drift, noise, bad points, random `max_ms`); archived values are measurements | `sdt_reconstruction_error_never_exceeds_deviation` (512 cases per run; checked once with 20 000 cases while developing) |
| exception: hold ≤ `e`, linear ≤ `2e` | `exception_deadband_bounds` |
| exception then SDT ≤ `2e + d` | `combined_pipeline_bound` |
| full plant pipeline (Sparkplug decode, deadband, spool through an outage, archive) ≤ `2e + d` on 90 minutes of 14 tags | `tests/pipeline.rs` |
| exact time-weighted average equals a 20 000-step numeric integral | `time_weighted_average_matches_numeric_integration` |
| the property test can tell good from bad | switching it to classic mode makes it fail (see the counter-example above) |
