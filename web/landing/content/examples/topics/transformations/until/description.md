`until()` sends STOP to the extractor, so the source stops producing. `filter()` lets the source
run to the end and discards as it goes - which never finishes on an unbounded source.
