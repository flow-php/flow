`joinEach()` asks a `DataFrameFactory` for the right side once per left batch, so the right side is
never loaded whole - here, the rows the database does not know about yet (`Join::left_anti`). Use
it when the right side is too large for [join()](/joins/join/#example).
