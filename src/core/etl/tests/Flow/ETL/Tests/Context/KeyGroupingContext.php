<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Bucketing\KeyGrouping;
use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Generator;

use function array_map;
use function array_values;
use function iterator_to_array;

final class KeyGroupingContext
{
    /**
     * @param list<Reference> $by
     * @param Generator<Rows> $input
     *
     * @return list<Rows>
     */
    public static function groups(array $by, Generator $input): array
    {
        return iterator_to_array(
            (new KeyGrouping(new KeyValues($by), new NativeHasher()))->group($input),
            preserve_keys: false,
        );
    }

    /**
     * The shape every grouping assertion needs: which values landed together, in group order.
     *
     * @param list<Reference> $by
     * @param Generator<Rows> $input
     *
     * @return list<list<mixed>>
     */
    public static function groupedValuesOf(array $by, Generator $input, string $column): array
    {
        return array_map(
            static fn(Rows $group): array => array_values(array_map(
                static fn(array $values): mixed => $values[$column],
                $group->toArray(),
            )),
            self::groups($by, $input),
        );
    }
}
