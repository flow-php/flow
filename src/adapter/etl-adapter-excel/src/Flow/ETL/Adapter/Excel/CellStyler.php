<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Schema\Definition;
use OpenSpout\Common\Entity\Style\Style;

interface CellStyler
{
    /**
     * Return a Style for the given cell, or null for default styling.
     *
     * @param mixed $value the value being written
     * @param Definition<mixed> $definition the column the value belongs to
     * @param int $rowNumber the 1-based row number in the sheet (1 = first data row, not header)
     * @param int $columnIndex the 0-based column index
     * @param string $sheetName the name of the sheet being written to
     */
    public function style(
        mixed $value,
        Definition $definition,
        int $rowNumber,
        int $columnIndex,
        string $sheetName,
    ): ?Style;
}
