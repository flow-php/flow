# ETL Adapter: Google Sheet

## Description

ETL Adapter that provides Extractor that works with Google sheets.

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.


## Installation 

``` 
composer require flow-php/etl-adapter-google-sheet:1.x@dev
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

## Development

In order to install dependencies please, launch following commands:

```bash
composer install
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.

## Needed parameters:
- `$auth_config` 

  - Create project: [console.cloud.google.com](https://console.cloud.google.com/projectcreate) choosing the right organization.
  - Enable google sheet API for created project on [api sheets.googleapis.com](https://console.cloud.google.com/apis/library/sheets.googleapis.com)
  - To work with google sheet enable it on [serviceaccounts](https://console.cloud.google.com/iam-admin/serviceaccounts/create) this will generate email for example `serviceaccounts@project.iam.gserviceaccount.com`
  - Generate json (auth config)  for created serviceaccounts on `Keys` tab.

- `$spreadsheet_document_id` Id need to be readed from the document we want to use, example for url `https://docs.google.com/spreadsheets/d/xyzID-for-documentxyz/edit` id is `xyzID-for-documentxyz`
- `$sheet_name` - Name of sheet from document you want to read.
