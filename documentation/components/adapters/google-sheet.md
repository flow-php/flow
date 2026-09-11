---
package: flow-php/etl-adapter-google-sheet
---

# ETL Adapter: Google Sheet

[PACKAGE_NAV]

[TOC]

Flow PHP's Google Sheet adapter reads a sheet of a Google Spreadsheet through the Sheets API v4 as a Flow extractor,
with the schema either declared or inferred from the sheet's first rows. The adapter is read-only: there is no loader.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-google-sheet.md).

## Extractor

```php
<?php

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet;
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_google_sheet($auth_config, $spreadsheet_document_id, $sheet_name))
    ->write(to_output())
    ->run();
```

## Needed parameters

- `$auth_config`

    - Create project: [console.cloud.google.com](https://console.cloud.google.com/projectcreate) choosing the right organization.
    - Enable google sheet API for created project on [api sheets.googleapis.com](https://console.cloud.google.com/apis/library/sheets.googleapis.com)
    - To work with google sheet enable it on [serviceaccounts](https://console.cloud.google.com/iam-admin/serviceaccounts/create) this will generate email for example `serviceaccounts@project.iam.gserviceaccount.com`
    - Generate json (auth config)  for created serviceaccounts on `Keys` tab.

- `$spreadsheet_document_id` ID needs to be readded from the document we want to use, example URL `https://docs.google.com/spreadsheets/d/xyzID-for-documentxyz/edit` ID is `xyzID-for-documentxyz`
- `$sheet_name` - Name of sheet from document you want to read.

## Schema

Declare it with `->withSchema(schema(...))`, or let the extractor infer it: without a declaration `schema()` reads the
first rows of the sheet (100 by default) through one `spreadsheets.values.get` and every batch carries that schema.
A sheet the sample already read in full is not fetched again. Every inferred column is nullable; strings that look like numbers, booleans and dates become
those types under the API's default `FORMATTED_VALUE`; with `->withOptions(['valueRenderOption' => 'UNFORMATTED_VALUE'])`
the API's own numbers and booleans are used as they are (dates arrive as serial numbers unless
`dateTimeRenderOption` is `FORMATTED_STRING`). `->inferSchema(infer_schema()->sampleSize(-1))` reads the whole sheet;
`->allStrings()` keeps every column a string. Empty cells are `null` (`->withEmptyToNull(false)` keeps them `""`).
A value past the sample that does not fit the inferred type fails the read with the column and row.
