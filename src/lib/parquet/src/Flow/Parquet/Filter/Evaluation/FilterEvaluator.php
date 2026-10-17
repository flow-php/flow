<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\LeafResolver;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Filter\Binding\RefusedLeaf;
use Flow\Parquet\Filter\Binding\ResolvedLeaf;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use WeakMap;

use function array_fill_keys;
use function array_values;
use function count;
use function reset;

/**
 * Column-wise, so a batch is filtered without building rows.
 */
final readonly class FilterEvaluator
{
    /**
     * @var WeakMap<In, array<string, true>>
     */
    private WeakMap $keys;

    /**
     * @var WeakMap<Comparison|In|IsNull|StartsWith, ResolvedLeaf>
     */
    private WeakMap $leaves;

    /**
     * Refuses at once what no row could answer, instead of matching nothing silently.
     */
    public function __construct(
        private Filter $filter,
        Schema $schema,
        Options $options,
    ) {
        /** @var WeakMap<Comparison|In|IsNull|StartsWith, ResolvedLeaf> $leaves */
        $leaves = new WeakMap();
        /** @var WeakMap<In, array<string, true>> $keys */
        $keys = new WeakMap();

        foreach (LeafResolver::resolve($filter, $schema, $options) as $node => $resolution) {
            if ($resolution instanceof RefusedLeaf) {
                throw new InvalidArgumentException($resolution->reason);
            }

            if (!$resolution instanceof ResolvedLeaf) {
                continue;
            }

            $leaves[$node] = $resolution;

            if (
                $node instanceof In
                && ($resolution->domain === LiteralDomain::Bytes || $resolution->domain === LiteralDomain::Uuid)
            ) {
                /** @var list<string> $values */
                $values = $resolution->literals;
                $keys[$node] = array_fill_keys($values, true);
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

        foreach ($this->filter->accept(
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

        foreach ($this->leaves as $leaf) {
            $roots[$leaf->root] = $leaf->root;
        }

        return array_values($roots);
    }
}
