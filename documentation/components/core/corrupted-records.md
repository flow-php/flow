# Corrupted Records

[DOC_LINK:/documentation/components/core/core.md]

A corrupted record is a source record that cannot become a row without lost or invented data. One example is a CSV
line or a sheet row with cells that do not match the header. Another example is a value that does not cast to the
declared type. A
read throws `CorruptedRecordException` at the first one. You can send them to a loader instead:

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\{from_json_lines, to_json, to_json_lines};
use function Flow\ETL\DSL\{count, data_frame, float_schema, int_schema, lit, not, ref, schema, to_branch, to_output};

$valid = ref('amount')->greaterThan(lit(0));

// read, route corrupted records, split valid and invalid rows
data_frame()
    ->read(from_csv('orders.csv')
        ->withSchema(schema(int_schema('id'), float_schema('amount')))
        ->withCorruptedRecords(to_json_lines('rejects/broken.jsonl')))
    ->write(to_branch($valid, to_json('clean.json')))
    ->write(to_branch(not($valid), to_json('rejects/invalid.json')))
    ->run();

// count: fail CI above a limit
$rejected = data_frame()->read(from_json_lines('rejects/broken.jsonl'))->count();

// report
data_frame()
    ->read(from_json_lines('rejects/broken.jsonl'))
    ->groupBy(ref('reason'))
    ->aggregate(count(ref('offset')))
    ->write(to_output())
    ->run();

// repair: a record with an extra field keeps its first two
data_frame()
    ->read(from_json_lines('rejects/broken.jsonl'))
    ->filter(ref('fields')->size()->greaterThan(lit(2)))
    ->select('offset', 'fields')
    ->withColumn('id', ref('fields')->arrayGet('0')->cast('int'))
    ->withColumn('amount', ref('fields')->arrayGet('1')->cast('float'))
    ->drop('fields')
    ->write(to_output())
    ->run();
```

`orders.csv`:

```
id,amount
1,10.5
2,abc
3,-4
4,7,extra
5,1.5
```

Output:

```
+----------------------+--------------+
|               reason | offset_count |
+----------------------+--------------+
| 3 fields, the header |            1 |
| column "amount": cou |            1 |
+----------------------+--------------+
2 rows
+--------+----+----------+
| offset | id |   amount |
+--------+----+----------+
|     28 |  4 | 7.000000 |
+--------+----+----------+
1 rows
```

## The routed row

| Column   | Type            | Value                                                                    |
|----------|-----------------|--------------------------------------------------------------------------|
| `uri`    | `?string`       | the file, null for `from_array()` and sequences                          |
| `offset` | `int`           | where the record starts, see below                                       |
| `line`   | `?int`          | if the read starts at the first byte of the file, the line of the record |
| `reason` | `string`        | `3 fields, the header has 2`, `column "amount": could not convert ...`   |
| `record` | `string`        | the record as the source holds it, else its cells or values as JSON      |
| `fields` | `?list<string>` | the cells a CSV, Excel or Google Sheet reader split, null for JSON       |

| Reader                    | `offset`                     |
|---------------------------|------------------------------|
| CSV, JSON lines           | byte offset of the record    |
| Excel, Google Sheet       | 1-based sheet row number     |
| JSON array                | 1-based element number       |
| `from_array()`, sequences | 1-based entry number         |

A run that routed records logs their number and destination. A corrupted record is not an extraction error. Without
`withCorruptedRecords()`, it fails the run, for each `ErrorHandler`. To skip it, route it. `limit()` and `offset()`
count only rows.
