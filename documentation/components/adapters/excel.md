---
package: flow-php/etl-adapter-excel
---

# Excel Adapter

[PACKAGE_NAV]

[TOC]

The Excel adapter reads and writes Excel data in ETL (Extract, Transform, Load) pipelines.

> Note: This adapter supports only local filesystem paths, because of limitations of the OpenSpout library that it
> uses. Remote filesystems (for example S3 or Azure Blob Storage) are not supported.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-excel.md).

## Extractor

```php
<?php

$rows = data_frame()
    ->read(from_excel('path/to/your/excel.xlsx'))
    ->fetch();
```

```php
<?php

$rows = data_frame()
    ->read(from_excel('path/to/your/excel.ods'))
    ->fetch();
```

```php
<?php

$rows = data_frame()
    ->read(
        from_excel('path/to/your/excel.xlsx')
            ->withSheetName('Sheet name')
    )
    ->fetch();
```

```php
<?php

$rows = data_frame()
    ->read(from_excel('path/to/your/excel.xlsx')->withHeader(false))
    ->offset(4)
    ->fetch();
```

`offset()` counts data rows. Here, a sheet read without a header therefore starts at row 5. With a header,
`->offset(3)` starts at row 5.

### Header and missing cells

```
header row  [' id ', '', 'name']   ->  columns ['id', 'e01', 'name']   (trimmed, a blank cell named e + its position)
record      ['1', 'a']             ->  ['id' => '1', 'e01' => 'a', 'name' => null]; `withMissingColumnsAsNull(false)` makes it a corrupted record
record      ['1', 'a', 'x', '']    ->  corrupted record (a non-empty cell past the header), an empty one is dropped
record      ['1', '', 'x']         ->  'e01' => null   (a present '' is null; `withConvertEmptyToNull(false)` keeps it '')
```

A corrupted record throws `CorruptedRecordException`, or goes to `->withCorruptedRecords($loader)`. For more
information, read [Corrupted Records](/documentation/components/core/corrupted-records.md). If a column of
`->withSchema()` is not in the header, the read throws `SchemaValidationException`.

A header cell that is not text (a date, a number) gets the same name as a blank header cell.

### Schema

Without a declared schema, `from_excel()` samples the workbook before it reads the first row. It decides one schema,
and each batch that it yields carries exactly that schema. `schema()` answers without a run of the pipeline:

```php
<?php

from_excel('path/to/your/excel.xlsx')->schema();
```

The default sample is the first `20 480` rows of up to `10` files. Each inferred column is nullable. If narrowing is
not safe, the column falls back to `string`.

A workbook has its own cell types, so inference reads those first. A numeric cell is `integer` or `float`, a boolean
cell is `boolean`, and a date-formatted cell is `date` or `datetime`. A date cell has no zone, so it reads as a UTC
wall clock. The value of `date.timezone` does not change this.
`withSchema(schema(datetime_schema('d', zone: 'Europe/Warsaw')))` converts it. A cell with text stays `string`. The
look of the text does not change this. `TRUE`, `12.9` and `2023-10-02` are all text that the author of the
spreadsheet chose not to type.

The exception is the types that a cell cannot hold at all. There is no uuid, json or timezone cell. Text is the only
way to write them, so inference narrows text for exactly those three types:

```
254d61c5-22c8-4407-83a2-76f1cab53af2   ->  uuid
{"street":"Main St"}                   ->  json
America/New_York                       ->  timezone
TRUE / 12.9 / 2023-10-02               ->  string
```

Change the sample, or make all columns strings:

```php
<?php

$rows = data_frame()
    ->read(
        from_excel('path/to/your/*.xlsx')
            ->inferSchema(infer_schema()->sampleSize(1_000)->filesToSniff(1)->allStrings())
    )
    ->fetch();
```

If the files of a glob have different headers, the read throws `InferredSchemaException`. The message names the file
that is different and the file that gave the schema. Declare the schema with `->withSchema(...)`, or read the files as
one wider schema with `->inferSchema(infer_schema()->unionByName())`.

If you know the schema, declare it. A declared schema never needs a sample:

```php
<?php

$rows = data_frame()
    ->read(
        from_excel('path/to/your/excel.xlsx')
            ->withSchema(schema(int_schema('id'), string_schema('name')))
    )
    ->fetch();
```

## Loader

### Basic Usage

```php
<?php

data_frame()
    ->read($extractor)
    ->write(to_excel('path/to/output.xlsx'))
    ->run();
```

### ODS Format

```php
<?php

use Flow\ETL\Adapter\Excel\IO\Loader\ExcelWriter;

data_frame()
    ->read($extractor)
    ->write(to_excel('path/to/output.ods')->withWriter(ExcelWriter::ODS))
    ->run();
```

### Custom Sheet Name

```php
<?php

data_frame()
    ->read($extractor)
    ->write(to_excel('path/to/output.xlsx')->withSheetName('MyData'))
    ->run();
```

### Sheet Name from a Column

Write each row to the sheet with the name of its `category` value:

```php
<?php

$loader = to_excel('path/to/output.xlsx')
    ->withSheetNameFromColumn('category');

data_frame()
    ->read($extractor)
    ->write($loader)
    ->run();
```

### Without Header Row

```php
<?php

data_frame()
    ->read($extractor)
    ->write(to_excel('path/to/output.xlsx')->withHeader(false))
    ->run();
```

### Header Styling

```php
<?php

use OpenSpout\Common\Entity\Style\Style;

$headerStyle = new Style(fontBold: true);

$loader = to_excel('path/to/output.xlsx')
    ->withHeaderStyle($headerStyle);

data_frame()
    ->read($extractor)
    ->write($loader)
    ->run();
```

### Cells

| Column                                       | Cell                                                                                   |
|----------------------------------------------|----------------------------------------------------------------------------------------|
| `string`, `integer`, `float`, `boolean`      | the value                                                                              |
| `datetime`, `date`                           | a date cell, displayed with `withDateTimeFormat()` / `withDateFormat()`                |
| `time`                                       | text, `withTimeFormat()`                                                               |
| `enum`                                       | the value of a backed enum, the name of a unit enum                                    |
| `uuid`, `json`                               | text                                                                                   |
| `xml`, `xml_element`, `html`, `html_element` | the markup, for example `<a b="1"><c></c></a>`                                         |
| `list`, `map`, `structure`                   | JSON text: a datetime element as `DATE_ATOM`, a time as microseconds, a float as `1.0` |

### Cell Styling

`$value` is the cell as it is written (read [Cells](#cells)), not the column value.

```php
<?php

use Flow\ETL\Adapter\Excel\IO\Loader\CellStyler;
use Flow\ETL\Values\Schema\Definition;
use OpenSpout\Common\Entity\Style\Style;

$cellStyler = new class implements CellStyler {
    public function style(
        mixed $value,
        Definition $definition,
        int $rowNumber,
        int $columnIndex,
        string $sheetName,
    ): ?Style {
        // Make first column bold
        if ($columnIndex === 0) {
            return new Style(fontBold: true);
        }

        return null;
    }
};

$loader = to_excel('path/to/output.xlsx')
    ->withCellStyler($cellStyler);

data_frame()
    ->read($extractor)
    ->write($loader)
    ->run();
```

### Custom Writer Options

For an advanced configuration, give OpenSpout options directly:

```php
<?php

use OpenSpout\Writer\XLSX\Options as XlsxOptions;

$options = new XlsxOptions(
    SHOULD_USE_INLINE_STRINGS: false,
    DEFAULT_COLUMN_WIDTH: 15.0,
    DEFAULT_ROW_HEIGHT: 20.0,
);

data_frame()
    ->read($extractor)
    ->write(to_excel('path/to/output.xlsx')->withWriterOptions($options))
    ->run();
```

For ODS format:

```php
<?php

use OpenSpout\Writer\ODS\Options as OdsOptions;

$options = new OdsOptions(
    DEFAULT_COLUMN_WIDTH: 15.0,
    DEFAULT_ROW_HEIGHT: 20.0,
);

data_frame()
    ->read($extractor)
    ->write(to_excel('path/to/output.ods')->withWriterOptions($options))
    ->run();
```

### Custom Date/Time Formats

Dates and datetimes are written as real date cells, so they read back as `date` / `datetime` and not as text.
`withDateFormat()` and `withDateTimeFormat()` take an Excel number format. They control how the cell is
displayed, never the value. `withTimeFormat()` still takes a PHP `DateInterval::format()` string, because a workbook
has no cell type that reads back as `time`.

```php
<?php

data_frame()
    ->read($extractor)
    ->write(
        to_excel('path/to/output.xlsx')
            ->withDateFormat('dd/mm/yyyy')
            ->withDateTimeFormat('dd/mm/yyyy hh:mm')
            ->withTimeFormat('%H:%I')
    )
    ->run();
```

Default formats:
- Date: `yyyy-mm-dd` (Excel number format)
- DateTime: `yyyy-mm-dd hh:mm:ss` (Excel number format)
- Time: `%H:%I:%S` (PHP `DateInterval::format()`)

