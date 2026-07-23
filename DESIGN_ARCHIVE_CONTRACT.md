# hecate-archive: the capture contract

**Status:** design, pre-implementation.
**Date:** 2026-07-23
**Shape:** an L2 mesh service in the `hecate-{warden,sentinel,news}` family.
**Serves:** `faber-ecosystem/plans/EXPLORATION_REAL_WORLD_STREAMS.md` section 6, and any
future sensor observing a stream whose history cannot be re-obtained.

---

## 0. Cardinality

**Many sensors, one archive.** The same shape as warden to sentinel: N lightweight
observers on N boxes, one collector that holds the record.

```
hecate-grid     (BE, GB, FI TSOs)   ┐
hecate-weather  (Open-Meteo, DWD)   ├─ publish observation facts ─→  hecate-archive
hecate-news     (RSS)               ┘                                (one service,
hecate-<next>                                                         many stores)
```

A sensor is cheap to add and cheap to run: it polls, it publishes, it holds nothing.
That is the property worth protecting, because the number of sensors is expected to
grow and the number of archives is not.

Consequences, taken deliberately:

- **The mesh is on the capture path.** Accepted, with the mitigations in section 4. The
  alternative (sensors spooling to local disk) puts a store in every sensor and makes
  each one heavier, which is the wrong trade at N sensors.
- **The archive is one service with many stores**, one directory tree per source. Per
  source retention, per source roll cadence, one implementation, one deployment.

---

## 1. Sensors publish RAW, and do not parse

The single most important line in this document.

An observation fact carries the **verbatim upstream payload**, not a parsed
interpretation of it. Parsing happens later, offline, against the archive.

Two reasons, and they point the same way:

1. **Correctness.** Insights 038 and 040, and the 047 provenance standard: an ingest
   that is wrong produces a downstream record that is wrong *and internally consistent*,
   and no analysis of that record can find the error. If the archive holds only parsed
   values, a parser bug is unrecoverable and undetectable. If it holds the bytes, every
   parser bug is a re-run.
2. **Lightness.** A sensor that does not parse is lighter than one that does. Not
   parsing is the cheap option, so the correct choice and the light choice coincide.

`hecate-news` is the exception that proves the rule: it parses and enriches because its
consumers are *minds*, which need a rendered `body` to reason about, in real time.
Capture sensors have no such consumer. Their consumer is a disk.

Where a live consumer does want parsed values (a realm dashboard, a mind), the sensor
may publish a parsed fact **in addition**, on a separate topic. It is a convenience
projection and it is never the record.

---

## 2. The observation fact

CBOR on the wire, CBOR at rest, byte-identical between them. Language-neutral, handles
binary blobs natively, still parseable in ten years by something that is not the BEAM.

```erlang
#{type           => observation,
  schema_v       => 1,                       %% envelope version, bump-only
  source         => <<"elia">>,              %% stable source id, never a URL
  dataset        => <<"ods134">>,            %% the source's own dataset id
  seq            => 918273,                  %% monotonic per {source, dataset}
  endpoint       => <<"https://opendata.elia.be/api/...">>,  %% full URL with query
  request_at     => 1753303499512,           %% ms, when we asked
  response_at    => 1753303500204,           %% ms, when it landed
  status         => 200,
  content_type   => <<"application/json">>,
  payload        => <<"...">>,               %% VERBATIM. No trimming, no reformatting.
  payload_sha256 => <<...:256>>,
  sensor_ref     => <<"a1b2c3d">>,           %% git sha of the publishing sensor
  from           => <<"hecate-grid">>}
```

Field notes, each of which is load-bearing:

- **No event time.** Event time lives inside the payload and extracting it is
  interpretation. The raw tier does not interpret. The whole discipline is in that
  omission.
- **`request_at` and `response_at` both**, so upstream latency is recoverable later.
  Free at write time, impossible to reconstruct afterwards. `response_at` is the
  observation time that experiments must filter on.
- **`seq` is what makes a hole visible.** Monotonic per source and dataset, assigned by
  the sensor. Without it a dropped fact is indistinguishable from a poll that never
  happened, and the archive silently lies by omission. See section 4.
- **`sensor_ref` is fable's objection 8 made concrete.** The capture harness is a runner
  and can be wrong. Every record carries the sha of the code that produced it, so a
  harness bug is bounded to a known interval instead of poisoning the archive.
- **Non-200 responses are published, not swallowed.** A gap in the archive must be
  distinguishable from an outage at the source, and both from our own crash. Dropping
  errors is the correlated-missingness trap by another route: sensors die *because* of
  the event, and a silent gap encodes "nothing happened" precisely when everything did.

Payloads are small, and this is measured rather than assumed. One Elia `ods161` row is
~324 bytes of JSON and a request round-trips in ~340 ms. The default seven-dataset Elia
set, with per-source intervals (one minute for the per-minute datasets, five for the
quarter-hourly ones) and overlap sized to a few intervals, is about **13 MB/day of raw
payload**, under 2 MB/day once the tape gzips it at seal.

Two things follow, both learned by measuring rather than by planning:

- **Overlap must be sized, not maximised.** The first draft asked for 100 rows every
  minute on per-minute data, which is a hundredfold overlap and would have been ~320
  MB/day. Recovery from a missed poll needs a few intervals of overlap, not a hundred.
- **Poll interval belongs to the source, not to the sensor.** A quarter-hourly dataset
  polled every minute is fifteen identical answers and fourteen wasted requests, landing
  on a free public service that can be lost by being abused.

A sensor whose payload does not fit comfortably on the wire is a sensor that should be
polling a narrower endpoint.

---

## 3. Archive layout

```
/bulk0/hecate-archive/<source>/<dataset>/<YYYY>/<MM>/<DD>/<source>-<YYYYMMDD>T<HH>.cbor.zst
                                                          <source>-<YYYYMMDD>T<HH>.sha256
                     <source>/<dataset>/GAPS.jsonl
                     splits.json
```

- Hourly roll, sealed on close, **never reopened**.
- Records length-prefixed (`<<Len:32/big, Cbor:Len/binary>>`), so a truncated tail from
  an unclean shutdown costs one record, not a segment.
- `.sha256` written at seal. A segment without one is unclean: flagged, never repaired.
- zstd at segment level, not record level.
- Retention: **forever**. It is the only tier and it is a few MB a day.

Idempotent on ingest: a fact whose `{source, dataset, seq}` is already present is
dropped, and `payload_sha256` mismatch on a duplicate seq is a hard error, logged loudly.
At-least-once delivery is therefore safe, and re-delivery after a reconnect is expected.

### Vertical slices

Matching the family shape (`hecate_warden`'s `sense_auth_log/`, `tarpit/`,
`announce_presence/`):

```
apps/hecate_archive/src/
  hecate_archive_app.erl
  hecate_archive_sup.erl
  hecate_archive_service.erl
  hecate_archive_facts.erl        %% what the archive itself publishes (health, gaps)
  collect_observations/           %% subscribe, dedupe, append
  seal_segments/                  %% roll, seal, checksum, verify
  report_gaps/                    %% the gap ledger, and publishing it as a fact
  announce_presence/
```

---

## 4. The mesh is on the capture path: making that honest

This is the accepted risk of the one-archive shape, and multi-hop pubsub propagation is
a known open defect after producer churn. It is not mitigated by hoping.

- **`seq` plus a gap ledger.** The archive tracks the last seq per `{source, dataset}`
  and appends every discontinuity to `GAPS.jsonl` with both seqs and wall-clock bounds.
  A hole becomes a recorded fact about the archive rather than an absence nobody sees.
  An experiment that spans a gap must say so.
- **The archive publishes its own gap facts**, so a gap is visible on the mesh and in
  the realm, not only on disk.
- **Overlapping windows on the pull side.** This one is BUILT (hecate-grid 0.1). Each
  poll asks for several intervals' worth of the most recent rows, so a poll that never
  happened, or whose publish was lost, is recovered by the next poll. It costs duplicate
  rows across consecutive records, which the replay parser removes by event time, at a
  point where the removal can be re-done. For pull sources this is a better mitigation
  than a replay path, because it needs no cooperation from either end.
- **A short ring buffer in the sensor** plus a replay request path, so the archive can
  ask for what it missed. **NOT BUILT in 0.1**, and named here as a known limit rather
  than left as an assumption: it needs a request path on both sides, and a half-built
  one would be worse than none. Until it exists, a lost publish on a push-only source is
  a permanent hole, visible in the ledger and not recoverable.
- **Verify propagation before trusting it.** Before a capture run is declared started,
  the archive's gap ledger must be clean over a stated soak period. Starting a
  multi-month capture on an unverified path is how you discover the transport bug in
  month three.

---

## 5. Parsed facts are materialised offline

There is no derived write path in the archive service. Parsed, scaled values are
produced by replaying segments at experiment time, with a parser whose sha is recorded
in the output.

```erlang
#{type         => grid_sample,
  source       => <<"elia">>,
  quantity     => imbalance_price,
  value        => -142350,                   %% scaled integer
  scale        => 1000,                      %% value / scale = the real number
  unit         => <<"EUR/MWh">>,
  event_at     => 1753300800000,             %% from the payload
  observed_at  => 1753303500204,             %% the fact's response_at
  derived_from => #{segment => <<"elia-20260723T14.cbor.zst">>,
                    offset  => 41293,
                    parser_ref => <<"a1b2c3d">>}}
```

- **Scale is per field, declared in the record.** Not a constant anyone has to remember.
  x1000 for price, power, temperature, frequency; x1e6 for lat/lng, where x1000 is 111 m
  and does not identify a station.
- `derived_from` points every value at the exact bytes it came from. That is what makes
  a parser bug a re-run instead of a retraction.
- No second write path means nothing to drift.

---

## 6. Splits committed at capture time

Append-only protects against revision, not against choosing windows after seeing the
data. Whichever windows and sources survive to analysis were selected post hoc unless
fixed first.

At capture start, `splits.json` is committed **to git**, so the commit date proves it
preceded the data:

```json
{"schema_v": 1,
 "committed_at": "2026-07-23",
 "block_days": 28,
 "purge_gap_days": 2,
 "blocks": [{"id": "b00", "from": "2026-08-01", "to": "2026-08-28"},
            {"id": "b01", "from": "2026-08-31", "to": "2026-09-27"}],
 "holdout": ["b05", "b09"]}
```

Blocks are declared forward in time, with purge gaps. An experiment cites block ids,
never dates. Adding blocks is allowed; editing or reordering one is not, and the git
history enforces it.

---

## 7. Open questions

1. `sensor_ref` is the sensor's sha. Should the archive also stamp its own sha per
   segment? Both can be wrong independently; probably yes, at seal time.
2. Ring buffer depth in section 4: minutes is the instinct, but the right number is
   whatever a relay restart costs, which is measurable rather than guessable.
3. Non-200 records will dominate the archive during an upstream outage. Cap them per
   hour per source, or accept the volume as an honest record of the outage?
