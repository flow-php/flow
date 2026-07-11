<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Partition;
use Flow\Floe\Exception\FloeException;
use Flow\Types\Exception\CastingException;

use function array_map;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class RowsValueMapper
{
    public const string VALUE_TYPE_KEY = 'flow.cache.value_type';

    private const string PARTITION_ORDER_KEY = 'flow.cache.partition_order';

    private const string VALUE_TYPE_ROW = 'row';

    private const string VALUE_TYPE_ROWS = 'rows';

    public static function metadataFor(Row|Rows $value): Metadata
    {
        $metadata = Metadata::with(
            self::VALUE_TYPE_KEY,
            $value instanceof Row ? self::VALUE_TYPE_ROW : self::VALUE_TYPE_ROWS,
        );

        if ($value instanceof Rows && $value->partitions()->count() > 0) {
            $metadata = $metadata->add(self::PARTITION_ORDER_KEY, array_map(
                static fn(Partition $p): string => $p->name,
                $value->partitions()->toArray(),
            ));
        }

        return $metadata;
    }

    /**
     * The value's partitions in the caller's original order (footer metadata) when recorded,
     * in footer order otherwise.
     *
     * @throws FloeException
     *
     * @return array<int, Partition>
     */
    public static function partitionsFrom(Footer $footer): array
    {
        $partitions = [];

        if ($footer->metadata->has(self::PARTITION_ORDER_KEY)) {
            try {
                /** @var list<string> $order */
                $order = $footer->metadata->getAs(self::PARTITION_ORDER_KEY, type_list(type_string()), []);
            } catch (CastingException $e) {
                throw new FloeException('Floe cache partition order metadata is malformed: ' . $e->getMessage(), 0, $e);
            }

            foreach ($order as $name) {
                $partitions[] = new Partition($name, $footer->partitions[$name]);
            }
        } else {
            foreach ($footer->partitions as $name => $value) {
                $partitions[] = new Partition($name, $value);
            }
        }

        return $partitions;
    }

    /**
     * Rebuilds the original Row|Rows from already-decoded (un-partitioned) rows and the file footer:
     * reattaches partitions in the caller's original order (footer metadata) and unwraps a tagged
     * single Row.
     *
     * @param array<int, Row> $rows
     *
     * @throws FloeException
     */
    public static function reconstructFrom(array $rows, Footer $footer): Row|Rows
    {
        $partitions = self::partitionsFrom($footer);

        $valueType = $footer->metadata->has(self::VALUE_TYPE_KEY)
            ? $footer->metadata->get(self::VALUE_TYPE_KEY)
            : self::VALUE_TYPE_ROWS;

        if ($valueType === self::VALUE_TYPE_ROW) {
            return $rows[0] ?? throw new FloeException('Floe value tagged as a single Row contained no rows');
        }

        return $partitions === [] ? new Rows(...$rows) : Rows::partitioned($rows, $partitions);
    }

    public static function wrap(Row|Rows $value): Rows
    {
        return $value instanceof Row ? new Rows($value) : $value;
    }
}
