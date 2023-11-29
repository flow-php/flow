<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Extractor;
use Google\Client;
use Google\Service\Sheets;

/**
 * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config
 * @param string $spreadsheet_id
 * @param string $sheet_name
 * @param bool $with_header
 * @param int $rows_per_page - how many rows per page to fetch from Google Sheets API
 * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
 */
function from_google_sheet(
    array|Sheets $auth_config,
    string $spreadsheet_id,
    string $sheet_name,
    bool $with_header = true,
    int $rows_per_page = 1000,
    array $options = [],
) : Extractor {
    if ($auth_config instanceof Sheets) {
        $sheets = $auth_config;
    } else {
        $client = new Client();
        $client->setScopes(Sheets::SPREADSHEETS_READONLY);
        $client->setAuthConfig($auth_config);
        $sheets = new Sheets($client);
    }

    return new GoogleSheetExtractor(
        $sheets,
        $spreadsheet_id,
        new Columns($sheet_name, 'A', 'Z'),
        $with_header,
        $rows_per_page,
        $options,
    );
}

/**
 * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config
 * @param string $spreadsheet_id
 * @param string $sheet_name
 * @param string $start_range_column
 * @param string $end_range_column
 * @param bool $with_header
 * @param int $rows_per_page - how many rows per page to fetch from Google Sheets API
 * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
 */
function from_google_sheet_columns(
    array|Sheets $auth_config,
    string $spreadsheet_id,
    string $sheet_name,
    string $start_range_column,
    string $end_range_column,
    bool $with_header = true,
    int $rows_per_page = 1000,
    array $options = [],
) : Extractor {
    if ($auth_config instanceof Sheets) {
        $sheets = $auth_config;
    } else {
        $client = new Client();
        $client->setScopes(Sheets::SPREADSHEETS_READONLY);
        $client->setAuthConfig($auth_config);
        $sheets = new Sheets($client);
    }

    return new GoogleSheetExtractor(
        $sheets,
        $spreadsheet_id,
        new Columns($sheet_name, $start_range_column, $end_range_column),
        $with_header,
        $rows_per_page,
        $options,
    );
}
