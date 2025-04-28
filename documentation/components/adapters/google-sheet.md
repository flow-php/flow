# ETL Adapter: Google Sheet

- [⬅️️ Back](../../introduction.md)
- [📚API Reference](/documentation/api/adapter/google-sheet)
- [📁Files](/documentation/api/adapter/google-sheet/indices/files.html)

Flow PHP's Adapter Google Sheet is a comprehensive library engineered to enable smooth interactions with Google Sheets
within your ETL (Extract, Transform, Load) workflows. This adapter is indispensable for developers looking to seamlessly
extract from or load data into Google Sheets, ensuring a coherent and reliable data transformation journey. By
leveraging the Adapter Google Sheet library, developers can utilize a robust set of features designed for precise
interaction with Google Sheets, simplifying complex data transformations and enhancing data processing efficiency. The
Adapter Google Sheet library encapsulates a wide range of functionalities, providing a streamlined API for managing
Google Sheets tasks, which is essential in modern data processing and transformation scenarios. This library reflects
Flow PHP's dedication to offering versatile and effective data processing solutions, making it an optimal choice for
developers dealing with Google Sheets in large-scale and data-intensive projects. With Flow PHP's Adapter Google Sheet,
managing Google Sheets data within your ETL workflows becomes a more simplified and efficient task, perfectly aligning
with the robust and adaptable nature of the Flow PHP ecosystem.

## Installation

```
composer require flow-php/etl-adapter-google-sheet:~--FLOW_PHP_VERSION--
```

## Extractor

```php
<?php

use function Flow\ETL\Adapter\GoogleSheet\{from_google_sheet};
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetRange;
use Flow\ETL\DSL\GoogleSheet;
use Flow\ETL\Flow;

$rows = (new Flow())
    ->read(from_google_sheet($auth_config, $spreadsheet_document_id, $sheet_name)))
    ->fetch();
```

## Required parameters

- `$auth_config`

    - Create a project: [console.cloud.google.com](https://console.cloud.google.com/projectcreate) choosing the right organization,
    - Enable Google Sheet API for a created project on [API sheets.googleapis.com](https://console.cloud.google.com/apis/library/sheets.googleapis.com),
    - To work with Google Sheet enable it on a [serviceaccounts](https://console.cloud.google.com/iam-admin/serviceaccounts/create) this will generate email for example `serviceaccounts@project.iam.gserviceaccount.com`,
    - Generate JSON (auth config), for created "serviceaccounts" on `Keys` tab.

- `$spreadsheet_document_id` ID needs to be readded from the document we want to use, example URL `https://docs.google.com/spreadsheets/d/xyzID-for-documentxyz/edit` ID is `xyzID-for-documentxyz`
- `$sheet_name` - Name of the sheet from the document you want to read.

## Loader

```php
<?php

use function Flow\ETL\Adapter\GoogleSheet\{google_create_spreadsheet,
    google_sheets,
    to_google_sheet};
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Google\Service\Sheets;

// Create a Google spreadsheet
$service = google_sheets(
    $auth_config,
    // Scope required to creating & updating spreadsheets
    Sheets::SPREADSHEETS
);

$sheetName = 'Flow test sheet';
$spreadsheet = google_create_spreadsheet(
    $service,
    'Flow test spreadsheet',
    $sheetName,
    'random@gmail.com'
);

(new Flow())
    ->process(
        new Rows(
            ...\array_map(
                fn (int $i) : Row => Row::create(
                    new Row\Entry\IntegerEntry('id', $i),
                    new Row\Entry\StringEntry('name', 'name_' . $i)
                ),
                \range(0, 10)
            )
        )
    )
    ->write(to_google_sheet($service, $spreadsheet->spreadsheetId, $sheetName))
    ->run();
```
