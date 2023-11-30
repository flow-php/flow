# ETL Adapter: Google Sheet

- [⬅️️ Back](../../introduction.md)

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
composer require flow-php/etl-adapter-google-sheet
```

## Extractor

```php
<?php

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetRange;
use Flow\ETL\DSL\GoogleSheet;
use Flow\ETL\Flow;

$rows = (new Flow())
    ->read(GoogleSheet::from($auth_config, $spreadsheet_document_id, $sheet_name)))
    ->fetch();
```

## Needed parameters

- `$auth_config`

    - Create project: [console.cloud.google.com](https://console.cloud.google.com/projectcreate) choosing the right organization.
    - Enable google sheet API for created project on [api sheets.googleapis.com](https://console.cloud.google.com/apis/library/sheets.googleapis.com)
    - To work with google sheet enable it on [serviceaccounts](https://console.cloud.google.com/iam-admin/serviceaccounts/create) this will generate email for example `serviceaccounts@project.iam.gserviceaccount.com`
    - Generate json (auth config)  for created serviceaccounts on `Keys` tab.

- `$spreadsheet_document_id` ID needs to be readded from the document we want to use, example URL `https://docs.google.com/spreadsheets/d/xyzID-for-documentxyz/edit` ID is `xyzID-for-documentxyz`
- `$sheet_name` - Name of sheet from document you want to read.
