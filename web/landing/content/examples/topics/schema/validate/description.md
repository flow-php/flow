`match()` checks the frame against a declared schema. `from_array()` infers every column nullable,
so the source is given the same schema - otherwise NOT NULL declarations never match.
