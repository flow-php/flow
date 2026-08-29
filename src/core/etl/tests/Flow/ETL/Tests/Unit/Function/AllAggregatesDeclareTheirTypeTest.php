<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use ErrorException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Function\Average;
use Flow\ETL\Function\Collect;
use Flow\ETL\Function\CollectUnique;
use Flow\ETL\Function\Count;
use Flow\ETL\Function\DenseRank;
use Flow\ETL\Function\First;
use Flow\ETL\Function\Last;
use Flow\ETL\Function\Max;
use Flow\ETL\Function\Min;
use Flow\ETL\Function\Rank;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\RowNumber;
use Flow\ETL\Function\StringAggregate;
use Flow\ETL\Function\Sum;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Tests\FlowTestCase;
use Throwable;

use function array_key_exists;
use function array_keys;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\collect_unique;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\dense_rank;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_agg;
use function Flow\ETL\DSL\sum;
use function Flow\Types\DSL\type_is_nullable;
use function restore_error_handler;
use function set_error_handler;

/**
 * Four members on 13 classes with no shared base means returns() can drift per class - and a single
 * missing type_optional() wrapper is invisible to any signature. This scan is the enforcement.
 */
final class AllAggregatesDeclareTheirTypeTest extends FlowTestCase
{
    /**
     * @var array<class-string, true> the producers whose returns() is an OptionalType
     */
    public const array NULLABLE = [
        Average::class => true,
        Collect::class => true,
        CollectUnique::class => true,
        First::class => true,
        Last::class => true,
        Max::class => true,
        Min::class => true,
        StringAggregate::class => true,
        Sum::class => true,
    ];

    public function test_every_producer_declares_and_round_trips(): void
    {
        $factories = self::factories();

        static::assertSame(array_keys($factories), ScalarFunctionClasses::producing());

        $nullable = 0;
        $bare = 0;

        foreach ($factories as $class => $factory) {
            $bound = (new ReferenceResolver())->resolve($factory(), self::inputSchema());

            $returns = $bound->returns();
            definition_from_type('out', $returns);

            if (array_key_exists($class, self::NULLABLE)) {
                static::assertTrue(type_is_nullable($returns), $class . ' must declare a nullable type');
                $nullable++;
            } else {
                static::assertFalse(type_is_nullable($returns), $class . ' must declare a NOT NULL type');
                $bare++;
            }
        }

        static::assertSame(9, $nullable);
        static::assertSame(4, $bare);
    }

    public function test_with_children_round_trips_in_both_directions(): void
    {
        foreach (self::factories() as $class => $factory) {
            $function = $factory();
            $children = $function->children();

            static::assertEquals($children, $function->withChildren($children)->children(), $class);

            if ($children === []) {
                continue;
            }

            // A PHP warning (undefined array key) is as loud as an exception here - both refuse.
            set_error_handler(static function (int $severity, string $message): never {
                throw new ErrorException($message, severity: $severity);
            });

            try {
                $function->withChildren([]);
            } catch (Throwable) {
                continue;
            } finally {
                restore_error_handler();
            }

            static::fail($class . '::withChildren([]) on a node with children must not silently succeed');
        }
    }

    /**
     * @return array<class-string, callable(): (AggregatingFunction|WindowFunction)>
     */
    public static function factories(): array
    {
        return [
            Average::class => static fn(): Average => average(ref('v')),
            Collect::class => static fn(): Collect => collect(ref('v')),
            CollectUnique::class => static fn(): CollectUnique => collect_unique(ref('v')),
            Count::class => static fn(): Count => count(ref('v')),
            DenseRank::class => static fn(): DenseRank => dense_rank(),
            First::class => static fn(): First => first(ref('v')),
            Last::class => static fn(): Last => last(ref('v')),
            Max::class => static fn(): Max => max(ref('v')),
            Min::class => static fn(): Min => min(ref('v')),
            Rank::class => static fn(): Rank => rank(),
            RowNumber::class => static fn(): RowNumber => row_number(),
            StringAggregate::class => static fn(): StringAggregate => string_agg(ref('v')),
            Sum::class => static fn(): Sum => sum(ref('v')),
        ];
    }

    public static function inputSchema(): Schema
    {
        return schema(int_schema('v'));
    }
}
