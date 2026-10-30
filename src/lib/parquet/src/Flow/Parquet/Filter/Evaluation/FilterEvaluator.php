<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Binding\ExactLeaf;
use Flow\Parquet\Filter\Binding\InexactLeaf;
use Flow\Parquet\Filter\Binding\RefusedLeaf;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\StartsWith;
use WeakMap;

use function array_values;
use function count;
use function reset;

/**
 * Column-wise, so a batch is filtered without building rows.
 */
final readonly class FilterEvaluator
{
    /**
     * @var WeakMap<In, array<int|string, true>>
     */
    private WeakMap $keys;

    /**
     * @var WeakMap<Comparison|In|IsNull|StartsWith, ExactLeaf|InexactLeaf>
     */
    private WeakMap $leaves;

    /**
     * Refuses at once what no row could answer, instead of matching nothing silently.
     */
    public function __construct(
        private BoundFilter $filter,
    ) {
        /** @var WeakMap<Comparison|In|IsNull|StartsWith, ExactLeaf|InexactLeaf> $leaves */
        $leaves = new WeakMap();
        /** @var WeakMap<In, array<int|string, true>> $keys */
        $keys = new WeakMap();

        foreach ($filter->leaves() as $node) {
            $resolution = $filter->leaf($node);

            if ($resolution instanceof RefusedLeaf) {
                throw new InvalidArgumentException($resolution->reason);
            }

            if (!$resolution instanceof ExactLeaf && !$resolution instanceof InexactLeaf) {
                continue;
            }

            $leaves[$node] = $resolution;

            if ($node instanceof In) {
                $literalKeys = [];

                foreach ($resolution->literals as $literal) {
                    $key = $resolution->domain->key($literal);

                    if ($key === null) {
                        continue 2;
                    }

                    $literalKeys[$key] = true;
                }

                $keys[$node] = $literalKeys;
            }
        }

        $this->leaves = $leaves;
        $this->keys = $keys;
    }

    /**
     * The indexes of the rows the filter is true for.
     *
     * @param array<string, list<mixed>> $batch decoded values by root column, every list the same length
     *
     * @return list<int>
     */
    public function mask(array $batch): array
    {
        $first = reset($batch);
        $matching = [];

        foreach ($this->filter
            ->filter()
            ->accept(
                new BatchTruth($this->leaves, $this->keys, $batch, $first === false ? 0 : count($first)),
            ) as $row => $truth) {
            if ($truth === true) {
                $matching[] = $row;
            }
        }

        return $matching;
    }

    /**
     * The root columns a batch must hold.
     *
     * @return list<string>
     */
    public function roots(): array
    {
        $roots = [];

        foreach ($this->leaves as $resolution) {
            $roots[$resolution->root] = $resolution->root;
        }

        return array_values($roots);
    }
}
