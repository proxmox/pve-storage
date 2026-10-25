# LVM missing-metadata fixtures

`lab1-nomda-orphan.txt` is an unchanged lvm2 2.03.31 capture from a node of a test cluster. It
includes a genuine orphan and a PV with no metadata copies whose metadata-bearing peer is not
visible on the observing node.

The wipe and scan tests extract the JSON between `== devices B,C` and `== full nohints` without
rewriting it. Both log warnings remain in the command output supplied to the parsers. The capture
also contains a full-report row summary and hinted output; those sections are preserved for
provenance, not passed to the JSON parsers.

`lab1-nomda-orphan-derived.colon` is a derived create-probe fixture, not captured command output.
It preserves the device names, VG names, UUIDs, and binary use flags from the captured JSON. The
`pv_size` column is a synthetic 1024 KiB because the captured command did not request a size.
Each line represents a separate device-restricted create probe, not a combined command response.

These fixtures establish the `[unknown]` VG spelling with use flag `1` and an empty VG with use
flag `0`. Contradictory flags, disappeared aliases, duplicate UUIDs, and malformed fields use
synthetic tests. Neither additional VG sentinels nor an `[unknown]` device name are established by
this capture.
