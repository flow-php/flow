---
package: flow-php/etl-adapter-google-sheet
---

# ETL Adapter: Google Sheet

[PACKAGE_NAV]

[TOC]

This adapter reads a Google Spreadsheet sheet. It is a Flow extractor that uses the Sheets API v4. You can declare
the schema, or the extractor infers it from the first rows of the sheet. The adapter is read-only: there is no loader.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-google-sheet.md).

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

### Header and missing cells

```
header row  [' id ', '', 'name']   ->  columns ['id', 'e01', 'name']   (trimmed, a blank cell named e + its position)
record      ['1', 'a']             ->  corrupted record (2 cells, the header has 3); `withMissingColumnsAsNull(true)` makes 'name' null
record      ['1', 'a', 'x', '']    ->  corrupted record (a non-empty cell past the header), an empty one is dropped
record      ['1', '', 'x']         ->  'e01' => null   (a present '' is null; `withEmptyToNull(false)` keeps it '')
```

A corrupted record throws `CorruptedRecordException`, or goes to `->withCorruptedRecords($loader)`. For more
information, read [Corrupted Records](/documentation/components/core/corrupted-records.md). If a column of
`->withSchema()` is not in the header, the read throws `SchemaValidationException`.

The extractor skips blank rows before the header, also under `withHeader(false)`.

## Needed parameters

- `$auth_config`

    - Create a project on [console.cloud.google.com](https://console.cloud.google.com/projectcreate). Choose the correct organization.
    - Enable the google sheet API for the project on [api sheets.googleapis.com](https://console.cloud.google.com/apis/library/sheets.googleapis.com).
    - Create a service account on [serviceaccounts](https://console.cloud.google.com/iam-admin/serviceaccounts/create). This generates an email, for example `serviceaccounts@project.iam.gserviceaccount.com`.
    - Generate the json (auth config) for the service account on the `Keys` tab.

- `$spreadsheet_document_id`: the ID of the document. Read it from the document URL. For the URL `https://docs.google.com/spreadsheets/d/xyzID-for-documentxyz/edit`, the ID is `xyzID-for-documentxyz`.
- `$sheet_name`: the name of the sheet in the document that you want to read.

## Schema

Declare it with `->withSchema(schema(...))`, or let the extractor infer it. Without a declaration, `schema()` reads
the first rows of the sheet (100 by default) through one `spreadsheets.values.get`, and each batch carries that schema.
If the sample already read the full sheet, the extractor does not fetch it again. Each inferred column is nullable.

- Under the default `FORMATTED_VALUE` of the API, strings that look like numbers, booleans and dates become those types.
- With `->withOptions(['valueRenderOption' => 'UNFORMATTED_VALUE'])`, the extractor uses the numbers and booleans of
  the API as they are. Dates then arrive as serial numbers, except with `dateTimeRenderOption` set to
  `FORMATTED_STRING`.
- `->inferSchema(infer_schema()->everyRow())` reads the full sheet.
- `->allStrings()` keeps each column a string.
- Empty cells are `null`. `->withEmptyToNull(false)` keeps them `""`.

A value after the sample that does not fit the inferred type fails the read. The error names the column and the row.
