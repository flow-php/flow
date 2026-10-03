`join()` matches rows of two frames on column values, as SQL does: `Join::inner`, `left`, `right` or
`left_anti`. `join_prefix` renames the right side's columns, so names never collide. The right
frame is read whole (held in memory, spilled past the memory limit); to look up only what each
batch needs, use [joinEach()](/joins/join_each/#example).
