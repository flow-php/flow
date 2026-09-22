<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Row\RawRowValues;
use Generator;
use IteratorAggregate;

/**
 * @implements IteratorAggregate<int, RawRowValues>
 */
final class WorkbookSheetSample implements IteratorAggregate
{
    /**
     * @var int<0, max>
     */
    private int $rows = 0;

    private bool $wholeSheet = false;

    /**
     * @param Generator<int, RawRowValues> $sheetRows
     */
    public function __construct(
        private readonly Generator $sheetRows,
    ) {}

    /**
     * @return Generator<int, RawRowValues>
     */
    public function getIterator(): Generator
    {
        foreach ($this->sheetRows as $rowValues) {
            $this->rows++;

            yield $rowValues;
        }

        $this->wholeSheet = true;
    }

    /**
     * @return int<0, max>
     */
    public function rows(): int
    {
        return $this->rows;
    }

    public function wholeSheet(): bool
    {
        return $this->wholeSheet;
    }
}
