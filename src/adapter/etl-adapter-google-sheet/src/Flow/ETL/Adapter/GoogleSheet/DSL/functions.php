<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type};
use Google\Client;
use Google\Service\Drive\Permission;
use Google\Service\{Drive, Sheets};
use Google\Service\Sheets\Spreadsheet;

/**
 * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config
 * @param string $spreadsheet_id
 * @param string $sheet_name
 * @param bool $with_header - @deprecated use withHeader method instead
 * @param int $rows_per_page - how many rows per page to fetch from Google Sheets API - @deprecated use withRowsPerPage method instead
 * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options - @deprecated use withOptions method instead
 */
#[DocumentationDSL(module: Module::GOOGLE_SHEET, type: Type::EXTRACTOR)]
function from_google_sheet(
    array|Sheets $auth_config,
    string $spreadsheet_id,
    string $sheet_name,
    bool $with_header = true,
    int $rows_per_page = 1000,
    array $options = [],
) : GoogleSheetExtractor {
    if ($auth_config instanceof Sheets) {
        $sheets = $auth_config;
    } else {
        $sheets = google_sheets($auth_config, Sheets::SPREADSHEETS_READONLY);
    }

    return (new GoogleSheetExtractor(
        $sheets,
        $spreadsheet_id,
        new Columns($sheet_name, 'A', 'Z'),
    ))->withHeader($with_header)
        ->withRowsPerPage($rows_per_page)
        ->withOptions($options);
}

/**
 * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config
 * @param string $spreadsheet_id
 * @param string $sheet_name
 * @param string $start_range_column
 * @param string $end_range_column
 * @param bool $with_header - @deprecated use withHeader method instead
 * @param int $rows_per_page - how many rows per page to fetch from Google Sheets API, default 1000 - @deprecated use withRowsPerPage method instead
 * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options - @deprecated use withOptions method instead
 */
#[DocumentationDSL(module: Module::GOOGLE_SHEET, type: Type::EXTRACTOR)]
function from_google_sheet_columns(
    array|Sheets $auth_config,
    string $spreadsheet_id,
    string $sheet_name,
    string $start_range_column,
    string $end_range_column,
    bool $with_header = true,
    int $rows_per_page = 1000,
    array $options = [],
) : GoogleSheetExtractor {
    if ($auth_config instanceof Sheets) {
        $sheets = $auth_config;
    } else {
        $sheets = google_sheets($auth_config, Sheets::SPREADSHEETS_READONLY);
    }

    return (new GoogleSheetExtractor(
        $sheets,
        $spreadsheet_id,
        new Columns($sheet_name, $start_range_column, $end_range_column),
    ))->withHeader($with_header)
        ->withRowsPerPage($rows_per_page)
        ->withOptions($options);
}

#[DocumentationDSL(module: Module::GOOGLE_SHEET, type: Type::LOADER)]
function to_google_sheet(
    array|Sheets $auth_config,
    string $spreadsheet_id,
    string $sheet_name,
    bool $with_header = true,
    ValueInputOption $value_input_option = ValueInputOption::USER_ENTERED,
) : GoogleSheetLoader {
    if ($auth_config instanceof Sheets) {
        $sheets = $auth_config;
    } else {
        $sheets = google_sheets($auth_config, Sheets::SPREADSHEETS);
    }

    return (new GoogleSheetLoader($sheets, $spreadsheet_id, $sheet_name))
        ->withHeader($with_header)
        ->withInputOption($value_input_option);
}

/**
 * @param array<mixed>|string $auth_config
 * @param array<string>|string $scopes
 */
#[DocumentationDSL(module: Module::GOOGLE_SHEET, type: Type::HELPER)]
function google_client(string|array $auth_config, string|array $scopes) : Client
{
    $client = new Client();
    $client->setScopes($scopes);
    $client->setAuthConfig($auth_config);

    return $client;
}

#[DocumentationDSL(module: Module::GOOGLE_SHEET, type: Type::HELPER)]
function google_sheets(string|array $auth_config, string|array $scopes) : Sheets
{
    return new Sheets(google_client($auth_config, $scopes));
}

#[DocumentationDSL(module: Module::GOOGLE_SHEET, type: Type::HELPER)]
function google_create_spreadsheet(
    array|Sheets $auth_config,
    string $spreadsheet_name,
    string $sheet_name,
    string $shareWithEmail,
) : Spreadsheet {
    if ($auth_config instanceof Sheets) {
        $sheets = $auth_config;
    } else {
        $sheets = google_sheets($auth_config, Sheets::SPREADSHEETS);
    }

    $spreadsheet = $sheets->spreadsheets->create(
        new Spreadsheet([
            'properties' => [
                'title' => $spreadsheet_name,
            ],
            'sheets' => [
                [
                    'properties' => [
                        'title' => $sheet_name,
                    ],
                ],
            ],
        ])
    );

    // Ensure required drive scope is applied
    $sheets->getClient()->addScope(Drive::DRIVE);

    $drive = new Drive($sheets->getClient());
    $drive->permissions->create(
        $spreadsheet->spreadsheetId,
        new Permission(
            [
                'type' => 'user',
                'role' => 'reader',
                'emailAddress' => $shareWithEmail,
            ]
        ),
        ['fields' => 'id']
    );

    return $spreadsheet;
}
