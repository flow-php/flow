<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Binding\PreImage\BytesPreImage;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function array_map;
use function array_reverse;
use function get_debug_type;
use function sprintf;
use function var_export;

use const DATE_ATOM;

/**
 * A filter leaf against a file schema: pruning and evaluation read the same column, domain, literals and physical
 * values.
 */
final readonly class LeafResolver
{
    public static function resolve(
        Comparison|In|IsNull|StartsWith $leaf,
        Schema $schema,
        DataConverter $converters,
    ): AbsentLeaf|RefusedLeaf|ResolvedLeaf {
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

        return new ResolvedLeaf($column, $domain, $root->name(), array_reverse($fields), $literals, match (true) {
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
        });
    }
}
