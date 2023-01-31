<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\GoogleSheet\Columns;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Extractor;
use Google\Client;
use Google\Service\Sheets;

class GoogleSheet
{
    /**
     * @param string $document_id
     * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string} $auth_config
     * @param int $rows_in_batch
     * @param bool $with_header
     *
     * @return Extractor
     */
    final public static function from(
        array $auth_config,
        string $spreadsheet_id,
        string $sheet_name,
        bool $with_header = true,
        int $rows_in_batch = 1000,
        string $row_entry_name = 'row'
    ) : Extractor {
        $client = new Client();
        $client->setScopes(Sheets::SPREADSHEETS_READONLY);
        $client->setAuthConfig($auth_config);

        return new GoogleSheetExtractor(new Sheets($client), $spreadsheet_id, new Columns($sheet_name, 'A', 'Z'), $with_header, $rows_in_batch, $row_entry_name);
    }

    /**
     * @param string $document_id
     * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string} $auth_config
     * @param int $rows_in_batch
     * @param bool $with_header
     *
     * @return Extractor
     */
    public static function from_columns(
        array $auth_config,
        string $spreadsheet_id,
        string $sheet_name,
        string $start_range_column,
        string $end_range_column,
        bool $with_header = true,
        int $rows_in_batch = 1000,
        string $row_entry_name = 'row'
    ) : Extractor {
        $client = new Client();
        $client->setScopes(Sheets::SPREADSHEETS_READONLY);
        $client->setAuthConfig($auth_config);

        return new GoogleSheetExtractor(new Sheets($client), $spreadsheet_id, new Columns($sheet_name, $start_range_column, $end_range_column), $with_header, $rows_in_batch, $row_entry_name);
    }
}
