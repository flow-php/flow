<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\Bucketing\KeyGrouping;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Rows;
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
            (new KeyGrouping(new KeyEncoder((new References(...$by))->names()), new AdaptiveBackend()))->group($input),
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
