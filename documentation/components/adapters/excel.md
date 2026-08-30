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
            ->withHeaders(false)
            ->withOffset(5)
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

Control how date, datetime, and time values are formatted in the output:

```php
<?php

data_frame()
    ->read($extractor)
    ->write(
        to_excel('path/to/output.xlsx')
            ->withDateFormat('d/m/Y')
            ->withDateTimeFormat('d/m/Y H:i')
            ->withTimeFormat('%H:%I')
    )
    ->run();
```

Default formats:
- Date: `Y-m-d`
- DateTime: `Y-m-d H:i:s`
- Time: `H:i:s`

