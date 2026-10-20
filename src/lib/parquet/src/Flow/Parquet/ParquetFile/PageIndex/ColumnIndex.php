<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\PageIndex;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ThriftModel\ColumnIndex as ThriftColumnIndex;

use function array_map;
use function array_values;
use function count;

/**
 * Entry i describes page i of the chunk's offset index; minValues/maxValues are the statistic bytes, empty for a
 * null page.
 */
final readonly class ColumnIndex
{
    /**
     * @param list<bool> $nullPages
     * @param list<string> $minValues
     * @param list<string> $maxValues
     * @param null|list<int> $nullCounts
     * @param null|list<int> $nanCounts
     */
    public function __construct(
        public array $nullPages,
        public array $minValues,
        public array $maxValues,
        public BoundaryOrder $boundaryOrder,
        public ?array $nullCounts = null,
        public ?array $nanCounts = null,
    ) {
        $pages = count($nullPages);

        if (
            count($minValues) !== $pages
            || count($maxValues) !== $pages
            || $nullCounts !== null && count($nullCounts) !== $pages
            || $nanCounts !== null && count($nanCounts) !== $pages
        ) {
            throw new InvalidArgumentException('Every list of a column index has one entry per page');
        }
    }

    public static function fromThrift(ThriftColumnIndex $index): self
    {
        return new self(
            array_values($index->null_pages ?? []),
            array_values($index->min_values ?? []),
            array_values($index->max_values ?? []),
            BoundaryOrder::tryFrom((int) $index->boundary_order) ?? throw new InvalidArgumentException('Unknown column index boundary order '
            . $index->boundary_order),
            $index->null_counts === null
                ? null
                : array_values(array_map(static fn($count): int => (int) $count, $index->null_counts)),
            $index->nan_counts === null
                ? null
                : array_values(array_map(static fn($count): int => (int) $count, $index->nan_counts)),
        );
    }

    public function pages(): int
    {
        return count($this->nullPages);
    }

    public function toThrift(): ThriftColumnIndex
    {
        return new ThriftColumnIndex([
            'null_pages' => $this->nullPages,
            'min_values' => $this->minValues,
            'max_values' => $this->maxValues,
            'boundary_order' => $this->boundaryOrder->value,
            'null_counts' => $this->nullCounts,
            'nan_counts' => $this->nanCounts,
        ]);
    }
}
