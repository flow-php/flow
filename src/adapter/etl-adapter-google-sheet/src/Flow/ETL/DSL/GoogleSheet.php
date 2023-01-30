<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetRange;
use Flow\ETL\Extractor;

class GoogleSheet
{
    /**
     * @param string $spreadsheet_id
     * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string} $auth_config
     * @param int $rows_in_batch
     * @param bool $with_header
     *
     * @return Extractor
     */
    final public static function from(
        string $spreadsheet_id,
        array $auth_config,
        GoogleSheetRange $initial_data_range,
        int $rows_in_batch = 1000,
        bool $with_header = true
    ) : Extractor {
        return GoogleSheetExtractor::create(
            $auth_config,
            $spreadsheet_id,
            $initial_data_range,
            $with_header,
            $rows_in_batch,
        );
    }
}
