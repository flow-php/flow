<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Inference;

use Generator;
use IteratorAggregate;

/**
 * @implements IteratorAggregate<int, array<array-key, mixed>>
 */
final class WorkbookSheetSample implements IteratorAggregate
{
    private bool $wholeSheet = false;

    /**
     * @param Generator<int, array<array-key, mixed>> $sheetRows
     */
    public function __construct(
        private readonly Generator $sheetRows,
    ) {}

    /**
     * @return Generator<int, array<array-key, mixed>>
     */
    public function getIterator(): Generator
    {
        foreach ($this->sheetRows as $rowValues) {
            yield $rowValues;
        }

        $this->wholeSheet = true;
    }

    public function wholeSheet(): bool
    {
        return $this->wholeSheet;
    }
}
