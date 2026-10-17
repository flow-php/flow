<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
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

use function array_map;
use function array_push;
use function array_reverse;
use function array_shift;
use function get_debug_type;
use function sprintf;
use function var_export;

use const DATE_ATOM;

/**
 * Each leaf of a filter against a file schema, once: pruning and evaluation read the same column, domain and
 * literals.
 */
final readonly class LeafResolver
{
    /**
     * @return WeakMap<Comparison|In|IsNull|StartsWith, LeafResolution> the leaves in the filter's order
     */
    public static function resolve(Filter $filter, Schema $schema, Options $options): WeakMap
    {
        $converters = DataConverter::initialize($options);
        /** @var WeakMap<Comparison|In|IsNull|StartsWith, LeafResolution> $leaves */
        $leaves = new WeakMap();
        $pending = [$filter];

        while ($pending !== []) {
            $node = array_shift($pending);

            match (true) {
                $node instanceof All, $node instanceof Any => array_push($pending, ...$node->filters),
                $node instanceof Not => $pending[] = $node->filter,
                $node instanceof Comparison,
                $node instanceof In,
                $node instanceof IsNull,
                $node instanceof StartsWith,
                    => $leaves[$node] = self::leaf($node, $schema, $converters),
                default => throw new InvalidArgumentException('Unsupported filter ' . get_debug_type($node)),
            };
        }

        return $leaves;
    }

    private static function leaf(
        Comparison|In|IsNull|StartsWith $leaf,
        Schema $schema,
        DataConverter $converters,
    ): LeafResolution {
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

        $converter = $converters->resolveConverter($column);
        $domain = LiteralDomain::forColumn($column, $converter);
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
                    self::type($column),
                    get_debug_type($literal),
                    $literal instanceof DateTimeImmutable ? $literal->format(DATE_ATOM) : var_export($literal, true),
                ));
            }
        }

        if ($leaf instanceof StartsWith && !$domain->holdsPrefix()) {
            return new RefusedLeaf(sprintf(
                'Column "%s" (%s) holds no strings for a prefix',
                $path,
                self::type($column),
            ));
        }

        $fields = [];
        $root = $column;

        while (($parent = $root->parent()) !== null && !$parent->schemaRoot) {
            $fields[] = $root->name();
            $root = $parent;
        }

        return new ResolvedLeaf($column, $converter, $domain, $root->name(), array_reverse($fields), $literals);
    }

    private static function type(FlatColumn $column): string
    {
        $logical = $column->logicalType();

        if ($logical === null) {
            return $column->type()->name;
        }

        $integer = $logical->integerData();
        $decimal = $logical->decimalData();
        $timestamp = $logical->timestampData();
        $time = $logical->timeData();

        return (
            $column->type()->name . ' ' . $logical->name() . match (true) {
                $integer !== null => sprintf(
                    '(%d, %s)',
                    $integer->bitWidth(),
                    $integer->isSigned() ? 'signed' : 'unsigned',
                ),
                $decimal !== null => sprintf('(%d, %d)', $decimal->precision(), $decimal->scale()),
                $timestamp !== null => sprintf(
                    '(%s, %s)',
                    $timestamp->unit()->name,
                    $timestamp->isAdjustedToUTC() ? 'UTC' : 'local',
                ),
                $time !== null => sprintf('(%s)', $time->unit()->name),
                default => '',
            }
        );
    }
}
