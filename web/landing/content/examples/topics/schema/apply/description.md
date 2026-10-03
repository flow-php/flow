`withSchema()` on a reader declares the column types instead of inferring them. Formats that carry
no types (CSV, JSON, XML, arrays) then never guess from a sample, and NOT NULL columns stay NOT NULL.
