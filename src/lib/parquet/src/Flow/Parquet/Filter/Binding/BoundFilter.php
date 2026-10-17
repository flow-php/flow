<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\PreImage\BytesPreImage;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use WeakMap;

use function array_values;

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
