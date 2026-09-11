---
package: flow-php/etl-adapter-excel
---

# Excel Adapter

[PACKAGE_NAV]

[TOC]

Flow PHP's Adapter Excel is a comprehensive library engineered to enable smooth interactions with Excel
within your ETL (Extract, Transform, Load) workflows. This adapter is indispensable for developers looking to seamlessly
extract from or load data into Excel, ensuring a coherent and reliable data transformation journey. By
leveraging the Adapter Excel library, developers can utilize a robust set of features designed for precise
interaction with Excel, simplifying complex data transformations and enhancing data processing efficiency. The
Adapter Excel library encapsulates a wide range of functionalities, providing a streamlined API for managing
Excel tasks, which is essential in modern data processing and transformation scenarios. This library reflects
Flow PHP's dedication to offering versatile and effective data processing solutions, making it an optimal choice for
developers dealing with Excel in large-scale and data-intensive projects. With Flow PHP's Adapter Excel,
managing Excel data within your ETL workflows becomes a more simplified and efficient task, perfectly aligning
with the robust and adaptable nature of the Flow PHP ecosystem.

> **Note:** This adapter only supports local filesystem paths due to limitations of the underlying OpenSpout library.
> Remote filesystems (S3, Azure Blob Storage, etc.) are not supported.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-excel.md).

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
    ->read(
        from_excel('path/to/your/excel.xlsx')
            ->withHeader(false)
            ->withOffset(5)
    )
    ->fetch();
```

Row numbers count from 1 and include the header row, so `->withOffset(5)` on a sheet with a header skips rows 2-4
and starts at row 5.

### Schema

Without a declared schema, `from_excel()` samples the workbook before the first row is read, decides one schema, and
every batch it yields carries exactly that schema. `schema()` answers without running the pipeline:

```php
<?php

from_excel('path/to/your/excel.xlsx')->schema();
```

The default sample is the first `20 480` rows of up to `10` files. Every inferred column is nullable, and where
narrowing is not safe the column floors to `string`.

A workbook has its own cell types, so inference reads those first: a numeric cell is `integer` or `float`, a boolean
cell is `boolean`, a date-formatted cell is `date` or `datetime`. A cell holding **text** stays `string` even when the
text looks like something else - `TRUE`, `12.9` and `2023-10-02` are all text a spreadsheet author chose not to type.

The exception is types a cell cannot hold at all. There is no uuid, json or timezone cell, so text is the only way to
write one and text is narrowed for exactly those three:

```
254d61c5-22c8-4407-83a2-76f1cab53af2   ->  uuid
{"street":"Main St"}                   ->  json
America/New_York                       ->  timezone
TRUE / 12.9 / 2023-10-02               ->  string
```

Tune the sample, or floor it:

```php
<?php

$rows = data_frame()
    ->read(
        from_excel('path/to/your/*.xlsx')
            ->inferSchema(infer_schema()->sampleSize(1_000)->filesToSniff(1)->allStrings())
    )
    ->fetch();
```

Reading a glob whose files carry different headers raises `InferredSchemaException`, naming the file that diverged and
the file the schema came from. Declare the schema with `->withSchema(...)`, or read the files as one wider schema with
`->inferSchema(infer_schema()->unionByName())`.

Declare the schema instead whenever it is known - a declared schema is never sampled for:

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

use Flow\ETL\Adapter\Excel\ExcelWriter;

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

### Dynamic Sheet Names from Entry

Route rows to different sheets based on entry values:

```php
<?php

$loader = to_excel('path/to/output.xlsx')
    ->withSheetNameFromEntry('category');

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

### Cell Styling

Apply custom styles to individual cells based on their value and the column they belong to:

```php
<?php

use Flow\ETL\Adapter\Excel\CellStyler;
use Flow\ETL\Schema\Definition;
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

For advanced configuration, pass OpenSpout options directly:

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

Dates and datetimes are written as real date cells, so they read back as `date` / `datetime` rather than as text.
`withDateFormat()` and `withDateTimeFormat()` take an **Excel number format** - they control how the cell is
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

