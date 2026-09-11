<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use DateTimeImmutable;
use Flow\ETL\Tests\Context\KeyGroupingContext;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class KeyGroupingTest extends FlowTestCase
{
    public function test_a_key_spanning_several_input_batches_lands_in_one_group(): void
    {
        $schema = schema(str_schema('k'), int_schema('v'));

        static::assertSame(
            [[1, 3], [2]],
            KeyGroupingContext::groupedValuesOf(
                [ref('k')],
                (static function () use ($schema): Generator {
                    yield rows($schema, row(['k' => 'a', 'v' => 1]), row(['k' => 'b', 'v' => 2]));
                    yield rows($schema, row(['k' => 'a', 'v' => 3]));
                })(),
                'v',
            ),
        );
    }

    public function test_a_list_column_is_a_usable_key(): void
    {
        $schema = schema(list_schema('tags', type_list(type_integer())), int_schema('v'));

        // Partition::fromValue() throws on a ListType; a hash does not care what the value is
        static::assertSame(
            [[1, 3], [2]],
            KeyGroupingContext::groupedValuesOf(
                [ref('tags')],
                (static function () use ($schema): Generator {
                    yield rows(
                        $schema,
                        row(['tags' => [1, 2], 'v' => 1]),
                        row(['tags' => [3], 'v' => 2]),
                        row(['tags' => [1, 2], 'v' => 3]),
                    );
                })(),
                'v',
            ),
        );
    }

    public function test_datetime_keys_keep_full_precision(): void
    {
        $schema = schema(datetime_schema('at'), int_schema('v'));

        // Partition::fromValue() truncates a datetime to Y-m-d; NativeHasher formats U.u, so two
        // times on the same day stay two groups - this is what b59 was
        static::assertSame(
            [[1, 3], [2, 4]],
            KeyGroupingContext::groupedValuesOf(
                [ref('at')],
                (static function () use ($schema): Generator {
                    yield rows(
                        $schema,
                        row(['at' => new DateTimeImmutable('2024-01-01 09:00:00'), 'v' => 1]),
                        row(['at' => new DateTimeImmutable('2024-01-01 21:00:00'), 'v' => 2]),
                        row(['at' => new DateTimeImmutable('2024-01-01 09:00:00'), 'v' => 3]),
                        row(['at' => new DateTimeImmutable('2024-01-01 21:00:00'), 'v' => 4]),
                    );
                })(),
                'v',
            ),
        );
    }

    public function test_empty_batches_are_skipped(): void
    {
        $schema = schema(str_schema('k'), int_schema('v'));

        static::assertSame(
            [[1]],
            KeyGroupingContext::groupedValuesOf(
                [ref('k')],
                (static function () use ($schema): Generator {
                    yield rows($schema);
                    yield rows($schema, row(['k' => 'a', 'v' => 1]));
                })(),
                'v',
            ),
        );
    }

    public function test_empty_input_yields_nothing(): void
    {
        static::assertSame(
            [],
            KeyGroupingContext::groupedValuesOf(
                [ref('k')],
                (static function (): Generator {
                    yield from [];
                })(),
                'v',
            ),
        );
    }

    public function test_numerically_equal_keys_of_different_types_collapse(): void
    {
        $schema = schema(float_schema('k'), int_schema('v'));

        // NativeHasher::normalize() casts every numeric to float, so 1 and 1.0 are the same key
        static::assertSame(
            [[1, 2]],
            KeyGroupingContext::groupedValuesOf(
                [ref('k')],
                (static function () use ($schema): Generator {
                    yield rows($schema, row(['k' => 1.0, 'v' => 1]), row(['k' => 1.0, 'v' => 2]));
                })(),
                'v',
            ),
        );
    }

    public function test_null_key_values_group_together(): void
    {
        $schema = schema(str_schema('k', nullable: true), int_schema('v'));

        static::assertSame(
            [[1, 3], [2]],
            KeyGroupingContext::groupedValuesOf(
                [ref('k')],
                (static function () use ($schema): Generator {
                    yield rows(
                        $schema,
                        row(['k' => null, 'v' => 1]),
                        row(['k' => 'a', 'v' => 2]),
                        row(['k' => null, 'v' => 3]),
                    );
                })(),
                'v',
            ),
        );
    }

    public function test_one_key_yields_one_group(): void
    {
        $schema = schema(str_schema('k'), int_schema('v'));

        static::assertSame(
            [[1, 2]],
            KeyGroupingContext::groupedValuesOf(
                [ref('k')],
                (static function () use ($schema): Generator {
                    yield rows($schema, row(['k' => 'a', 'v' => 1]), row(['k' => 'a', 'v' => 2]));
                })(),
                'v',
            ),
        );
    }

    public function test_the_group_keeps_the_schema_of_the_first_non_empty_batch(): void
    {
        $schema = schema(str_schema('k'), int_schema('v'));

        static::assertEquals(
            $schema,
            KeyGroupingContext::groups(
                [ref('k')],
                (static function () use ($schema): Generator {
                    yield rows($schema, row(['k' => 'a', 'v' => 1]));
                })(),
            )[0]->schema(),
        );
    }
}
