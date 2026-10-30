"""arrow_schema_hints.parquet: a file whose ARROW:schema carries the string encodings the reader's canonical hint maps
to LargeUtf8 / LargeBinary - a dictionary string column (what pyarrow writes for pandas categoricals), a string-view
and a binary-view column (what Polars writes), a plain string, a plain binary - plus a uuid(16) column the hint must
leave alone. Run from the polars shell: nix-shell --arg with-rust true --arg with-polars true --run "python $0"."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

rows = 5
table = pa.table(
    {
        "dict": pa.array(["a", "b", None, "a", "c"]).dictionary_encode(),
        "view": pa.array(["one", None, "", "four", "five"], type=pa.string_view()),
        "binary_view": pa.array([b"\x00\x01", None, b"", b"\xff", b"xyz"], type=pa.binary_view()),
        "plain": pa.array(["p", "q", None, "", "r"]),
        "binary": pa.array([b"\x02", None, b"", b"\xfe", b"bin"]),
        "uuid": pa.array(
            [bytes(range(16)), None, bytes([0xAB] * 16), bytes(range(16, 32)), bytes([0] * 16)],
            type=pa.uuid(),
        ),
        "id": pa.array(list(range(rows)), type=pa.int64()),
    }
)

pq.write_table(table, Path(__file__).with_suffix(".parquet"), compression="snappy", version="2.6")
print(pq.read_schema(Path(__file__).with_suffix(".parquet")))
