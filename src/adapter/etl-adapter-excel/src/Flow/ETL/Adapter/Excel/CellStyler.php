<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Row\Entry;
use OpenSpout\Common\Entity\Style\Style;

interface CellStyler
{
    /**
     * Return a Style for the given cell, or null for default styling.
     *
     * @param Entry<mixed> $entry the entry being written
     * @param int $rowNumber the 1-based row number in the sheet (1 = first data row, not header)
     * @param int $columnIndex the 0-based column index
     * @param string $sheetName the name of the sheet being written to
     */
    public function style(Entry $entry, int $rowNumber, int $columnIndex, string $sheetName) : ?Style;
}
