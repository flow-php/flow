<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Binding\PreImage\BytesPreImage;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use WeakMap;

use function array_filter;
use function array_values;
use function count;
use function get_debug_type;

final readonly class BoundFilter
{
    /**
     * @param WeakMap<Comparison|In|IsNull|StartsWith, BoundLeaf> $leaves
     */
    private function __construct(
        private Filter $filter,
        private WeakMap $leaves,
    ) {}

    /**
     * Never throws for a reason that depends on the file: a leaf the file cannot answer is Absent or Unbound.
     */
    public static function bind(Filter $filter, Schema $fileSchema, Options $options): self
    {
        /** @var WeakMap<Comparison|In|IsNull|StartsWith, BoundLeaf> $leaves */
        $leaves = new WeakMap();

        foreach (LeafResolver::resolve($filter, $fileSchema, $options) as $leaf => $resolution) {
            $leaves[$leaf] = self::bindLeaf($leaf, $resolution);
        }

        return new self($filter, $leaves);
    }

    /**
     * The bound columns, each once.
     *
     * @return list<FlatColumn>
     */
    public function columns(): array
    {
        $columns = [];

        foreach ($this->leaves as $leaf) {
            if ($leaf instanceof Bound) {
                $columns[$leaf->column->flatPath()] = $leaf->column;
            }
        }

        return array_values($columns);
    }

    public function leaf(Comparison|In|IsNull|StartsWith $leaf): BoundLeaf
    {
        return $this->leaves[$leaf] ?? throw new InvalidArgumentException('The leaf is not part of the bound filter');
    }

    public function filter(): Filter
    {
        return $this->filter;
    }

    public function bound(Comparison|In|IsNull|StartsWith $leaf): Bound|Absent
    {
        $binding = $this->leaf($leaf);

        if ($binding instanceof Bound || $binding instanceof Absent) {
            return $binding;
        }

        throw new InvalidArgumentException(
            'Rows are evaluated on bound leaves only, the leaf on "' . $leaf->column->path . '" is not bound',
        );
    }

    /**
     * The root conjuncts rowFilter() leaves out, which the caller evaluates; null when there is none.
     */
    public function residual(): ?Filter
    {
        $residual = array_values(array_filter(
            self::conjuncts($this->filter),
            fn(Filter $conjunct): bool => !$this->evaluable($conjunct),
        ));

        return match (count($residual)) {
            0 => null,
            1 => $residual[0],
            default => new All(...$residual),
        };
    }

    /**
     * The root conjuncts whose leaves are all bound or absent, the ones rows can be evaluated on, with their leaves'
     * bindings; itself when every conjunct is, null when none is.
     */
    public function rowFilter(): ?self
    {
        $conjuncts = self::conjuncts($this->filter);
        $kept = array_values(array_filter($conjuncts, $this->evaluable(...)));

        if ($kept === []) {
            return null;
        }

        if ($kept === $conjuncts) {
            return $this;
        }

        /** @var WeakMap<Comparison|In|IsNull|StartsWith, BoundLeaf> $leaves */
        $leaves = new WeakMap();

        foreach ($kept as $conjunct) {
            foreach (self::leaves($conjunct) as $leaf) {
                $leaves[$leaf] = $this->leaf($leaf);
            }
        }

        return new self(count($kept) === 1 ? $kept[0] : new All(...$kept), $leaves);
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
    private static function leaves(Filter $filter): array
    {
        if ($filter instanceof All || $filter instanceof Any) {
            $leaves = [];

            foreach ($filter->filters as $child) {
                foreach (self::leaves($child) as $leaf) {
                    $leaves[] = $leaf;
                }
            }

            return $leaves;
        }

        return match (true) {
            $filter instanceof Not => self::leaves($filter->filter),
            $filter instanceof Comparison,
            $filter instanceof In,
            $filter instanceof IsNull,
            $filter instanceof StartsWith,
                => [$filter],
            default => throw new InvalidArgumentException('Unsupported filter ' . get_debug_type($filter)),
        };
    }

    private function evaluable(Filter $conjunct): bool
    {
        foreach (self::leaves($conjunct) as $leaf) {
            if ($this->leaf($leaf) instanceof Unbound) {
                return false;
            }
        }

        return true;
    }

    private static function bindLeaf(Comparison|In|IsNull|StartsWith $leaf, LeafResolution $resolution): BoundLeaf
    {
        if ($resolution instanceof AbsentLeaf) {
            return new Absent();
        }

        if (!$resolution instanceof ResolvedLeaf || $resolution->column->type() === PhysicalType::INT96) {
            return new Unbound();
        }

        $column = $resolution->column;
        $order = PhysicalOrder::forColumn($column);

        if ($leaf instanceof IsNull) {
            return new Bound($column, $order, new PhysicalIntervals([PhysicalInterval::unbounded()]));
        }

        $preImage = PreImages::forColumn($column, $resolution->converter);

        $intervals = match (true) {
            $preImage === null => null,
            $leaf instanceof Comparison => PhysicalIntervals::preImage(
                $preImage,
                $order,
                $leaf->operator,
                $resolution->literals[0],
            ),
            $leaf instanceof In => PhysicalIntervals::preImageOfSet($preImage, $order, $resolution->literals),
            $leaf instanceof StartsWith => $preImage instanceof BytesPreImage
                ? $preImage->startsWith($leaf->prefix)
                : null,
        };

        return $intervals === null ? new Unbound() : new Bound($column, $order, $intervals);
    }
}
