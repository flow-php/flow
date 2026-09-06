<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Double;

use Google\Service\Sheets;

/**
 * Google\Service\Resource's constructor is deliberately not called - the stub answers from the spreadsheet it was
 * handed and never touches the HTTP client the parent would build.
 */
final class StubSpreadsheetsResource extends Sheets\Resource\Spreadsheets
{
    public int $calls = 0;

    public function __construct(
        private readonly Sheets\Spreadsheet $spreadsheet,
    ) {}

    public function get($spreadsheetId, $optParams = []): Sheets\Spreadsheet
    {
        $this->calls++;

        return $this->spreadsheet;
    }
}
