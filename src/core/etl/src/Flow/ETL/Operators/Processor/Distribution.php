<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Kernels\HashJoin\JoinKeys;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Operators\Processor;

final readonly class Distribution
{
    /**
     * @param list<string> $keys rows with equal keys meet in one process; [] when every row meets in one process
     * @param null|list<string> $rightKeys the right input's keys, pairwise with $keys; only a two-input join
     * @param list<SortColumn> $order the order every range keeps; [] unless ordered
     * @param null|Processor $partial runs per split before the exchange
     * @param null|Processor $final runs after the exchange; null when the distributing processor itself finishes
     */
    private function __construct(
        public array $keys,
        public ?array $rightKeys,
        public array $order,
        public ?Processor $partial,
        public ?Processor $final,
    ) {}

    /**
     * @param non-empty-list<string> $keys
     */
    public static function clustered(array $keys, ?Processor $partial = null, ?Processor $final = null): self
    {
        return new self($keys, null, [], $partial, $final);
    }

    public static function coPartitioned(JoinKeys $keys): self
    {
        return new self($keys->left, $keys->right, [], null, null);
    }

    /**
     * @param non-empty-list<SortColumn> $order
     */
    public static function ordered(array $order): self
    {
        return new self([], null, $order, null, null);
    }

    public static function single(?Processor $partial = null, ?Processor $final = null): self
    {
        return new self([], null, [], $partial, $final);
    }
}
