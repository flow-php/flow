<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Row\RawRowValues;

final readonly class GoogleSheetSample
{
    /**
     * @param list<string> $names the header (or generated names) - known even when no data row follows
     * @param list<RawRowValues> $rows
     * @param bool $wholeSheet the range reached the last row of the grid, so $rows IS the sheet and a read can be
     *                         served from it instead of fetching the same rows again
     */
    public function __construct(
        public array $names,
        public array $rows,
        public bool $wholeSheet,
    ) {}
}
