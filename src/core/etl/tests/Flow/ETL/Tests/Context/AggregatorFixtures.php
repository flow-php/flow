<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Foundation\Calculator\Rounding;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\Average;
use Flow\ETL\Functions\Collect;
use Flow\ETL\Functions\CollectUnique;
use Flow\ETL\Functions\Count;
use Flow\ETL\Functions\First;
use Flow\ETL\Functions\Last;
use Flow\ETL\Functions\Max;
use Flow\ETL\Functions\Min;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\StringAggregate;
use Flow\ETL\Functions\Sum;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\SortOrder;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\collect_unique;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_agg;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use function Flow\Types\DSL\type_instance_of;

/**
 * One instance of every shipped aggregator over a 6-row fixture (a null in the middle, a duplicate value).
 */
final class AggregatorFixtures
{
    /**
     * @return list<class-string<AggregatingFunction>>
     */
    public static function classes(): array
    {
        return [
            Average::class,
            Collect::class,
            CollectUnique::class,
            Count::class,
            First::class,
            Last::class,
            Max::class,
            Min::class,
            StringAggregate::class,
            Sum::class,
        ];
    }

    /**
     * @param class-string<AggregatingFunction> $class
     */
    public static function instance(string $class): AggregatingFunction
    {
        return match ($class) {
            Average::class => average(ref('v'), 3, Rounding::HALF_EVEN, true)->over(window()->partitionBy(ref('s'))),
            Collect::class => collect(ref('v')),
            CollectUnique::class => collect_unique(ref('v')),
            Count::class => count(ref('v'))->over(window()->partitionBy(ref('s'))),
            First::class => first(ref('v')),
            Last::class => last(ref('v')),
            Max::class => max(ref('v')),
            Min::class => min(ref('v')),
            StringAggregate::class => string_agg(ref('s'), '|', SortOrder::DESC),
            Sum::class => sum(ref('v'), exact: true)->over(window()->partitionBy(ref('s'))),
            default => throw InvalidArgumentException::because('%s has no fixture', $class),
        };
    }

    /**
     * The instance bound to rows()' schema, so returns() and partialType() are known.
     *
     * @param class-string<AggregatingFunction> $class
     */
    public static function resolved(string $class): AggregatingFunction
    {
        return type_instance_of(AggregatingFunction::class)->assert((new ReferenceResolver())->resolve(
            self::instance($class),
            self::schema(),
        ));
    }

    /**
     * $function bound to $schema, as a group-by holds it, so merge() can check a partial against partialType().
     */
    public static function bound(AggregatingFunction $function, Schema $schema): AggregatingFunction
    {
        return type_instance_of(AggregatingFunction::class)->assert((new ReferenceResolver())->resolve(
            $function,
            $schema,
        ));
    }

    public static function rows(Backend $backend): Rows
    {
        return array_to_rows(
            [
                ['v' => 3, 's' => 'c'],
                ['v' => 1, 's' => 'a'],
                ['v' => null, 's' => 'b'],
                ['v' => 3, 's' => 'c'],
                ['v' => 7, 's' => 'd'],
                ['v' => 2, 's' => 'a'],
            ],
            self::schema(),
            $backend,
        );
    }

    public static function schema(): Schema
    {
        return schema(int_schema('v', true), str_schema('s'));
    }
}
