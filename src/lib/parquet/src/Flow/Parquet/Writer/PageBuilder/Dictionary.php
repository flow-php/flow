<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder;

final readonly class Dictionary
{
    /**
     * @param array<int, mixed> $dictionary
     * @param array<int, int> $indices the indices of the values the dictionary was last extended with
     * @param array<int|string, int> $keys each entry's lookup key to its index
     */
    public function __construct(
        public array $dictionary,
        public array $indices,
        public array $keys = [],
    ) {}
}
