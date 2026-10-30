<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
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
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use WeakMap;

use function array_map;
use function array_reverse;
use function array_values;
use function count;
use function get_debug_type;
use function sprintf;
use function var_export;

use const DATE_ATOM;

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
     * @param WeakMap<Comparison|In|IsNull|StartsWith, AbsentLeaf|RefusedLeaf|ExactLeaf|InexactLeaf> $leaves
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

    public static function resolve(
        Comparison|In|IsNull|StartsWith $leaf,
        Schema $schema,
        DataConverter $converters,
    ): AbsentLeaf|RefusedLeaf|ExactLeaf|InexactLeaf {
        $path = $leaf->column->path;

        if (!$schema->has($path)) {
            return new AbsentLeaf();
        }

        $column = $schema->get($path);

        if (!$column instanceof FlatColumn) {
            return new RefusedLeaf('Filter column "' . $path . '" is not a leaf column');
        }

        if ($column->maxRepetitionsLevel() > 0) {
            return new RefusedLeaf(
                'Filter column "' . $path . '" lies inside a list or a map, where a row holds no single value',
            );
        }

        [$domain, $preImage] = LiteralDomain::forColumn($column, $converters->resolveConverter($column));
        $literals = array_map($domain->normalise(...), match (true) {
            $leaf instanceof Comparison => [$leaf->value],
            $leaf instanceof In => $leaf->values,
            default => [],
        });

        foreach ($literals as $literal) {
            if (!$domain->holds($literal)) {
                return new RefusedLeaf(sprintf(
                    'Column "%s" (%s) cannot hold the %s literal %s',
                    $path,
                    $column->ddl()['type'],
                    get_debug_type($literal),
                    $literal instanceof DateTimeImmutable ? $literal->format(DATE_ATOM) : var_export($literal, true),
                ));
            }
        }

        if ($leaf instanceof StartsWith && !$domain->holdsPrefix()) {
            return new RefusedLeaf(sprintf(
                'Column "%s" (%s) holds no strings for a prefix',
                $path,
                $column->ddl()['type'],
            ));
        }

        $fields = [];
        $root = $column;

        while (($parent = $root->parent()) !== null && !$parent->schemaRoot) {
            $fields[] = $root->name();
            $root = $parent;
        }

        $order = PhysicalOrder::forColumn($column);

        $intervals = match (true) {
            $leaf instanceof IsNull => new PhysicalIntervals($order, []),
            $preImage === null => null,
            $leaf instanceof Comparison => PhysicalIntervals::preImage(
                $preImage,
                $order,
                $leaf->operator,
                $literals[0],
            ),
            $leaf instanceof In => PhysicalIntervals::preImageOfSet($preImage, $order, $literals),
            $leaf instanceof StartsWith => $preImage instanceof BytesPreImage
                ? $preImage->startsWith($leaf->prefix)
                : null,
        };

        return $intervals === null
            ? new InexactLeaf($column, $domain, $root->name(), array_reverse($fields), $literals)
            : new ExactLeaf($column, $domain, $root->name(), array_reverse($fields), $literals, $intervals);
    }

    /**
     * Never throws for a reason that depends on the file: a leaf the file cannot answer is absent or refused.
     */
    public static function bind(Filter $filter, Schema $fileSchema, Options $options): self
    {
        $converters = DataConverter::initialize($options);
        /** @var WeakMap<Comparison|In|IsNull|StartsWith, AbsentLeaf|RefusedLeaf|ExactLeaf|InexactLeaf> $leaves */
        $leaves = new WeakMap();

        foreach (self::leavesOf($filter) as $node) {
            $leaves[$node] ??= self::resolve($node, $fileSchema, $converters);
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

            if ($resolution instanceof ExactLeaf) {
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

    public function leaf(Comparison|In|IsNull|StartsWith $node): AbsentLeaf|RefusedLeaf|ExactLeaf|InexactLeaf
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

    public function rowLeaf(Comparison|In|IsNull|StartsWith $node): AbsentLeaf|ExactLeaf
    {
        $resolution = $this->leaf($node);

        if ($resolution instanceof AbsentLeaf || $resolution instanceof ExactLeaf) {
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

            if (!$resolution instanceof AbsentLeaf && !$resolution instanceof ExactLeaf) {
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
