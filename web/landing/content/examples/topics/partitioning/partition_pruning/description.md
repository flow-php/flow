A `filter()` that reads only partition columns is pushed into the reader: directories whose path
fails the predicate (`color=green`) are never opened.
