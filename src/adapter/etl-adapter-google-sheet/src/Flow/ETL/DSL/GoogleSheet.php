<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetRange;
use Flow\ETL\Extractor;
use Google\Service\Sheets;

class GoogleSheet
{
    /**
     * @param string $spreadsheetIds
     * @param int $rows_in_batch
     * @param bool $with_header
     *
     * @return Extractor
     */
    final public static function from(
        string $spreadsheetIds,
        Sheets $service,
        GoogleSheetRange $initialDataRange,
        int $rows_in_batch = 1000,
        bool $with_header = true
    ) : Extractor {
        return new GoogleSheetExtractor(
            $service,
            $spreadsheetIds,
            $initialDataRange,
            $with_header,
            $rows_in_batch,
        );
    }
}
