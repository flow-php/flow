# Excel Adapter 

- [⬅️️ Back](../../introduction.md)
- [📚API Reference](/documentation/api/adapter/excel)
- [📁Files](/documentation/api/adapter/excel/indices/files.html)

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

## Installation

``` 
composer require flow-php/etl-adapter-excel:~--FLOW_PHP_VERSION--
```

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
