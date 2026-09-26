import datetime as dt
from decimal import Decimal as D

import pyarrow as pa
import pyarrow.parquet as pq

UTC = dt.timezone.utc


def ts(*parts, **kwargs):
    return dt.datetime(*parts, **kwargs)


int_backed = pa.table({
    'ts_ms': pa.array([ts(2020, 1, 2, 3, 4, 5, 678000), ts(1969, 12, 31, 23, 59, 58, 500000), None], type=pa.timestamp('ms')),
    'ts_us': pa.array([ts(2020, 1, 2, 3, 4, 5, 678901), ts(1900, 1, 1, 0, 0, 0, 1), None], type=pa.timestamp('us')),
    'ts_ns': pa.array([1577934245678901234, -1500, None], type=pa.timestamp('ns')),
    'ts_us_utc': pa.array([ts(2020, 1, 2, 3, 4, 5, 678901, tzinfo=UTC), ts(1969, 12, 31, 23, 59, 58, 500000, tzinfo=UTC), None], type=pa.timestamp('us', tz='UTC')),
    'ts_us_far': pa.array([ts(2262, 4, 11, 23, 47, 16, 854775), ts(1677, 9, 21, 0, 12, 43, 145225), None], type=pa.timestamp('us')),
    'd': pa.array([dt.date(2020, 1, 2), dt.date(1969, 12, 31), None], type=pa.date32()),
    't_ms': pa.array([dt.time(3, 4, 5, 678000), dt.time(0, 0, 0), None], type=pa.time32('ms')),
    't_us': pa.array([dt.time(3, 4, 5, 678901), dt.time(23, 59, 59, 999999), None], type=pa.time64('us')),
    't_ns': pa.array([11045678901234, 999, None], type=pa.time64('ns')),
    'dec9': pa.array([D('12345.67'), D('-12345.67'), None], type=pa.decimal128(9, 2)),
    'dec18': pa.array([D('1234567890123456.78'), D('-0.01'), None], type=pa.decimal128(18, 2)),
})
pq.write_table(int_backed, 'output/logical_types.parquet', store_decimal_as_integer=True)

fixed_len = pa.table({
    'dec9': pa.array([D('12345.67'), D('-12345.67'), None], type=pa.decimal128(9, 2)),
    'dec18': pa.array([D('1234567890123456.78'), D('-0.01'), None], type=pa.decimal128(18, 2)),
    'dec38': pa.array([D('1234567890123456789.0123456789'), D('-12345.6700000000'), None], type=pa.decimal128(38, 10)),
    'dec50': pa.array([D('1234567890123456789012345678901234567890.0123456789'), D('-12345.6700000000'), None], type=pa.decimal256(50, 10)),
})
pq.write_table(fixed_len, 'output/decimals_fixed_len.parquet')
