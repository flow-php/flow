<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use WeakMap;

use function array_values;
use function count;
use function get_debug_type;

final readonly class BoundFilter
{
    /**
     * @var list<Filter> the root conjuncts rows are evaluated on: every leaf absent or answered exactly
     */
    private array $rowConjuncts;

    /**
     * @var list<Filter> the other root conjuncts
     */
    private array $residualConjuncts;

    /**
     * @param WeakMap<Comparison|In|IsNull|StartsWith, AbsentLeaf|RefusedLeaf|ResolvedLeaf> $leaves
     */
    private function __construct(
        private Filter $filter,
        private WeakMap $leaves,
    ) {
        $rowConjuncts = [];
        $residualConjuncts = [];

        foreach (self::conjuncts($filter) as $conjunct) {
            if ($this->answersRows($conjunct)) {
                $rowConjuncts[] = $conjunct;
            } else {
                $residualConjuncts[] = $conjunct;
            }
        }

        $this->rowConjuncts = $rowConjuncts;
        $this->residualConjuncts = $residualConjuncts;
    }

    /**
     * Never throws for a reason that depends on the file: a leaf the file cannot answer is absent or refused.
     */
    public static function bind(Filter $filter, Schema $fileSchema, Options $options): self
    {
        $converters = DataConverter::initialize($options);
        /** @var WeakMap<Comparison|In|IsNull|StartsWith, AbsentLeaf|RefusedLeaf|ResolvedLeaf> $leaves */
        $leaves = new WeakMap();

        foreach (self::leavesOf($filter) as $node) {
            $leaves[$node] ??= LeafResolver::resolve($node, $fileSchema, $converters);
        }

        return new self($filter, $leaves);
    }

    /**
     * The columns of the leaves answered exactly, each once.
     *
     * @return list<FlatColumn>
     */
    public function columns(): array
    {
        $columns = [];

        foreach (self::leavesOf($this->filter) as $node) {
            $resolution = $this->leaf($node);

            if ($resolution instanceof ResolvedLeaf && $resolution->intervals !== null) {
                $columns[$resolution->column->flatPath()] = $resolution->column;
            }
        }

        return array_values($columns);
    }

    public function filter(): Filter
    {
        return $this->filter;
    }

    public function answersEveryConjunct(): bool
    {
        return $this->residualConjuncts === [];
    }

    public function leaf(Comparison|In|IsNull|StartsWith $node): AbsentLeaf|RefusedLeaf|ResolvedLeaf
    {
        return $this->leaves[$node] ?? throw new InvalidArgumentException('The leaf is not part of the bound filter');
    }

    /**
     * The filter's leaves, depth first.
     *
     * @return list<Comparison|In|IsNull|StartsWith>
     */
    public function leaves(): array
    {
        return self::leavesOf($this->filter);
    }

    /**
     * The root conjuncts rowFilter() leaves out, which the caller evaluates; null when there is none.
     */
    public function residual(): ?self
    {
        return $this->over($this->residualConjuncts);
    }

    /**
     * The root conjuncts rows are evaluated on, with their leaves' resolutions; null when none is.
     */
    public function rowFilter(): ?self
    {
        return $this->residualConjuncts === [] && $this->rowConjuncts !== [] ? $this : $this->over($this->rowConjuncts);
    }

    public function rowLeaf(Comparison|In|IsNull|StartsWith $node): AbsentLeaf|ResolvedLeaf
    {
        $resolution = $this->leaf($node);

        if (
            $resolution instanceof AbsentLeaf
            || $resolution instanceof ResolvedLeaf && $resolution->intervals !== null
        ) {
            return $resolution;
        }

        throw new InvalidArgumentException(
            'Rows are evaluated on leaves answered exactly, the leaf on "' . $node->column->path . '" is not',
        );
    }

    /**
     * Nested All flattened: each is a conjunct of the root.
     *
     * @return list<Filter>
     */
    private static function conjuncts(Filter $filter): array
    {
        if (!$filter instanceof All) {
            return [$filter];
        }

        $conjuncts = [];

        foreach ($filter->filters as $child) {
            foreach (self::conjuncts($child) as $conjunct) {
                $conjuncts[] = $conjunct;
            }
        }

        return $conjuncts;
    }

    /**
     * @return list<Comparison|In|IsNull|StartsWith>
     */
    private static function leavesOf(Filter $filter): array
    {
        if ($filter instanceof All || $filter instanceof Any) {
            $leaves = [];

            foreach ($filter->filters as $child) {
                foreach (self::leavesOf($child) as $node) {
                    $leaves[] = $node;
                }
            }

            return $leaves;
        }

        return match (true) {
            $filter instanceof Not => self::leavesOf($filter->filter),
            $filter instanceof Comparison,
            $filter instanceof In,
            $filter instanceof IsNull,
            $filter instanceof StartsWith,
                => [$filter],
            default => throw new InvalidArgumentException('Unsupported filter ' . get_debug_type($filter)),
        };
    }

    private function answersRows(Filter $conjunct): bool
    {
        foreach (self::leavesOf($conjunct) as $node) {
            $resolution = $this->leaf($node);

            if (
                !$resolution instanceof AbsentLeaf
                && (!$resolution instanceof ResolvedLeaf || $resolution->intervals === null)
            ) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param list<Filter> $conjuncts
     */
    private function over(array $conjuncts): ?self
    {
        return match (count($conjuncts)) {
            0 => null,
            1 => new self($conjuncts[0], $this->leaves),
            default => new self(new All(...$conjuncts), $this->leaves),
        };
    }
}
