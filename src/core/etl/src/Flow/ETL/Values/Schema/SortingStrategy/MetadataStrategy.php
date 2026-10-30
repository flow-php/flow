<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\SortingStrategy;

use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\SortingStrategy;
use Flow\ETL\Values\SortOrder;

final readonly class MetadataStrategy implements SortingStrategy
{
    public function __construct(
        private string $key,
        private SortOrder $order = SortOrder::ASC,
    ) {}

    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    public function compare(Definition $left, Definition $right): int
    {
        $leftHas = $left->metadata()->has($this->key);
        $rightHas = $right->metadata()->has($this->key);

        if ($leftHas && $rightHas) {
            $comparison = $this->order === SortOrder::ASC
                ? $left->metadata()->get($this->key) <=> $right->metadata()->get($this->key)
                : $right->metadata()->get($this->key) <=> $left->metadata()->get($this->key);

            if ($comparison !== 0) {
                return $comparison;
            }

            return $left->name() <=> $right->name();
        }

        if ($leftHas) {
            return -1;
        }

        if ($rightHas) {
            return 1;
        }

        return $left->name() <=> $right->name();
    }
}
