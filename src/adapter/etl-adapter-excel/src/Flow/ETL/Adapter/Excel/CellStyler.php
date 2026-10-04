<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use DateTimeInterface;
use Flow\ETL\Schema\Definition;
use OpenSpout\Common\Entity\Style\Style;

interface CellStyler
{
    /**
     * Return a Style for the given cell, or null for default styling.
     *
     * @param null|bool|DateTimeInterface|float|int|string $value the cell as it is written: a backed enum's value or a
     *                                                              unit enum's name, a uuid as text, a list / map /
     *                                                              structure as JSON text, a date or datetime as a
     *                                                              DateTimeInterface
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
