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
     * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config
     * @param int $rows_in_batch
     * @param bool $with_header
     * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
     *
     * @return Extractor
     */
    final public static function from(
        array|Sheets $auth_config,
        string $spreadsheet_id,
        string $sheet_name,
        bool $with_header = true,
        int $rows_in_batch = 1000,
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
            $rows_in_batch,
            $options,
        );
    }

    /**
     * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config
     * @param int $rows_in_batch
     * @param bool $with_header
     * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
     *
     * @return Extractor
     */
    public static function from_columns(
        array|Sheets $auth_config,
        string $spreadsheet_id,
        string $sheet_name,
        string $start_range_column,
        string $end_range_column,
        bool $with_header = true,
        int $rows_in_batch = 1000,
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
            $rows_in_batch,
            $options,
        );
    }
}
