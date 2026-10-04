Two `append()` runs leave two files in each partition (`products.csv` and
`products_<suffix>.csv`). The glob `color=*/*.csv` reads both, and `color` comes back from the path.
