<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Bucketing;

use DateTimeImmutable;
use Flow\ETL\Tests\Context\KeyGroupingContext;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
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
                    yield array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2]], $schema);
                    yield array_to_rows([['k' => 'a', 'v' => 3]], $schema);
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
                    yield array_to_rows([
                        ['tags' => [1, 2], 'v' => 1],
                        ['tags' => [3], 'v' => 2],
                        ['tags' => [1, 2], 'v' => 3],
                    ], $schema);
                })(),
                'v',
            ),
        );
    }

    public function test_datetime_keys_keep_full_precision(): void
    {
        $schema = schema(datetime_schema('at'), int_schema('v'));

        static::assertSame(
            [[1, 3], [2, 4]],
            KeyGroupingContext::groupedValuesOf(
                [ref('at')],
                (static function () use ($schema): Generator {
                    yield array_to_rows([
                        ['at' => new DateTimeImmutable('2024-01-01 09:00:00'), 'v' => 1],
                        ['at' => new DateTimeImmutable('2024-01-01 21:00:00'), 'v' => 2],
                        ['at' => new DateTimeImmutable('2024-01-01 09:00:00'), 'v' => 3],
                        ['at' => new DateTimeImmutable('2024-01-01 21:00:00'), 'v' => 4],
                    ], $schema);
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
                    yield array_to_rows([['k' => 'a', 'v' => 1]], $schema);
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

    public function test_zero_and_negative_zero_keys_collapse(): void
    {
        $schema = schema(float_schema('k'), int_schema('v'));

        static::assertSame(
            [[1, 2]],
            KeyGroupingContext::groupedValuesOf(
                [ref('k')],
                (static function () use ($schema): Generator {
                    yield array_to_rows([['k' => 0.0, 'v' => 1], ['k' => -0.0, 'v' => 2]], $schema);
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
                    yield array_to_rows([
                        ['k' => null, 'v' => 1],
                        ['k' => 'a', 'v' => 2],
                        ['k' => null, 'v' => 3],
                    ], $schema);
                })(),
                'v',
            ),
        );
    }

    public function test_many_distinct_keys_group_like_few(): void
    {
        $schema = schema(int_schema('k'), int_schema('v'));
        $many = 25;
        $batches = static function (int $keys) use ($schema): Generator {
            $first = [];
            $second = [];

            for ($k = 0; $k < $keys; $k++) {
                $first[] = ['k' => $k, 'v' => $k];
                $second[] = ['k' => $keys - 1 - $k, 'v' => $keys + $k];
            }

            yield array_to_rows($first, $schema);
            yield array_to_rows($second, $schema);
        };

        $groups = KeyGroupingContext::groupedValuesOf([ref('k')], $batches($many), 'v');

        static::assertCount($many, $groups);
        static::assertSame([0, (2 * $many) - 1], $groups[0]);
        static::assertSame([$many - 1, $many], $groups[$many - 1]);
        static::assertSame([[0, 3], [1, 2]], KeyGroupingContext::groupedValuesOf([ref('k')], $batches(2), 'v'));
    }

    public function test_numeric_strings_gathered_by_value_stay_apart_with_nulls_together(): void
    {
        $schema = schema(str_schema('k', true), int_schema('v'));

        static::assertSame(
            [[1], [2, 3, 5], [4], [6]],
            KeyGroupingContext::groupedValuesOf(
                [ref('k')],
                (static function () use ($schema): Generator {
                    yield array_to_rows([
                        ['k' => '1', 'v' => 1],
                        ['k' => null, 'v' => 2],
                        ['k' => null, 'v' => 3],
                        ['k' => '1.0', 'v' => 4],
                    ], $schema);
                    yield array_to_rows([['k' => null, 'v' => 5], ['k' => '1.00', 'v' => 6]], $schema);
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
                    yield array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'a', 'v' => 2]], $schema);
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
                    yield array_to_rows([['k' => 'a', 'v' => 1]], $schema);
                })(),
            )[0]->schema(),
        );
    }
}
