<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Loader\File\PartitionRouter;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Partitions;

use function iterator_to_array;
use function usort;

final class PartitionRoutingContext
{
    /**
     * Routing order follows a hash map, so every assertion needs the groups in a stable order.
     *
     * @return list<array{Partitions, Rows}>
     */
    public static function route(Partitioning $partitioning, Rows $rows): array
    {
        $groups = iterator_to_array((new PartitionRouter($partitioning))->route($rows), preserve_keys: false);

        usort($groups, static fn(array $a, array $b): int => self::sortKey($a[0]) <=> self::sortKey($b[0]));

        return $groups;
    }

    public static function sortKey(Partitions $partitions): string
    {
        $key = '';

        foreach ($partitions as $partition) {
            $key .= $partition->segment() . '/';
        }

        return $key;
    }
}
