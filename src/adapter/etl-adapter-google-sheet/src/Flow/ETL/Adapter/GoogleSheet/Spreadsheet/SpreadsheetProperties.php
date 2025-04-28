<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Spreadsheet;

final class SpreadsheetProperties
{
    public function __construct(
        public int $sheetId,
        public int $rowCount,
    ) {
    }
}
