<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Inference;

use Flow\ETL\Adapter\Excel\IO\Extractor\WorkbookSheet;
use Generator;
use IteratorAggregate;

use function is_array;

/**
 * @implements IteratorAggregate<int, array<array-key, mixed>>
 */
final class WorkbookSheetSample implements IteratorAggregate
{
    private bool $wholeSheet = false;

    /**
     * @param Generator<int, array<array-key, mixed>> $sheetRows the sheet's raw rows, decoded by $sheet
     */
    public function __construct(
        private readonly WorkbookSheet $sheet,
        private readonly Generator $sheetRows,
    ) {}

    /**
     * The rows of the records that are not corrupted.
     *
     * @return Generator<int, array<array-key, mixed>>
     */
    public function getIterator(): Generator
    {
        foreach ($this->sheetRows as $cells) {
            $values = $this->sheet->decode($cells);

            if (is_array($values)) {
                yield $values;
            }
        }

        $this->wholeSheet = true;
    }

    public function wholeSheet(): bool
    {
        return $this->wholeSheet;
    }
}
