<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

final class All implements Comparison
{
    /**
     * @var array<Comparison>
     */
    private array $comparisons;

    public function __construct(Comparison $comparison, Comparison ...$comparisons)
    {
        $this->comparisons = \array_merge([$comparison], $comparisons);
    }

    public function compare(Row $left, Row $right) : bool
    {
        foreach ($this->comparisons as $comparison) {
            if (!$comparison->compare($left, $right)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return array<Reference>
     */
    public function left() : array
    {
        $entries = [];

        foreach ($this->comparisons as $comparison) {
            $entries[] = $comparison->left();
        }

        return \array_unique(\array_merge(...$entries));
    }

    /**
     * @return array<Reference>
     */
    public function right() : array
    {
        $entries = [];

        foreach ($this->comparisons as $comparison) {
            $entries[] = $comparison->right();
        }

        return \array_unique(\array_merge(...$entries));
    }
}
