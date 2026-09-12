`array_expand()` turns one row holding an array into one row per element. The other columns are
repeated. It needs the element type, so a bare `json` column is not enough.

It cannot be used inside another `array_expand()`, or in `filter()`, `until()`, `duplicateRow()`,
`aggregate()`, `over()` and `onEach()`.
