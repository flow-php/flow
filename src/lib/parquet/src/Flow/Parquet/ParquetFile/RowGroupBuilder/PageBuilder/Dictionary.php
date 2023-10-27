<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

final class Dictionary
{
    /**
     * @param array<int, mixed> $dictionary
     * @param array<int, int> $indices
     */
    public function __construct(
        public readonly array $dictionary,
        public readonly array $indices,
    ) {
    }
}
