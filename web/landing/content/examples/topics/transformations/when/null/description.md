`when()` replaces nulls with `0`. Without `else`, a row that does not match gets `null`, so
`else: ref('value')` keeps the value it had.
