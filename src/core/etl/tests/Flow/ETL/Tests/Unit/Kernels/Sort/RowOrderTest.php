<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Sort;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Kernels\Sort\RowOrder;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\SortDatasetMother;
use Flow\ETL\Values\NullsOrder;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml_element;

use const NAN;
use const SORT_NUMERIC;
use const SORT_REGULAR;
use const SORT_STRING;

final class RowOrderTest extends FlowTestCase
{
    public function test_flag_orders_text_physicals_by_bytes_the_others_numerically_and_the_rest_regularly(): void
    {
        static::assertSame(SORT_STRING, RowOrder::flag(type_optional(type_string())));
        static::assertSame(SORT_STRING, RowOrder::flag(type_uuid()));
        static::assertSame(SORT_NUMERIC, RowOrder::flag(type_integer()));
        static::assertSame(SORT_NUMERIC, RowOrder::flag(type_datetime()));
        static::assertSame(SORT_REGULAR, RowOrder::flag(type_list(type_integer())));
    }

    public function test_nulls_sort_first_ascending_and_last_descending(): void
    {
        $rows = array_to_rows([['v' => 3], ['v' => null], ['v' => -1], ['v' => null]], schema(int_schema('v', true)));
        $ascending = new RowOrder([ref('v')->asc()]);
        $descending = new RowOrder([ref('v')->desc()]);

        static::assertSame([1, 3, 2, 0], $ascending->permutation($ascending->keys($rows), 4));
        static::assertSame([0, 2, 1, 3], $descending->permutation($descending->keys($rows), 4));
    }

    public function test_nan_sorts_after_every_value_and_ties_with_nan(): void
    {
        $rows = array_to_rows([
            ['v' => 1.0],
            ['v' => NAN],
            ['v' => -1.0],
            ['v' => NAN],
            ['v' => null],
        ], schema(float_schema('v', true)));
        $ascending = new RowOrder([ref('v')->asc()]);
        $descending = new RowOrder([ref('v')->desc()]);

        static::assertSame([4, 2, 0, 1, 3], $ascending->permutation($ascending->keys($rows), 5));
        static::assertSame([1, 3, 0, 2, 4], $descending->permutation($descending->keys($rows), 5));
        static::assertSame(0, $ascending->compare($ascending->keys($rows), 1, $ascending->keys($rows), 3));
    }

    public function test_negative_zero_ties_with_zero(): void
    {
        $rows = array_to_rows([['v' => 0.0], ['v' => -0.0], ['v' => 0.0]], schema(float_schema('v')));
        $order = new RowOrder([ref('v')->desc()]);

        static::assertSame([0, 1, 2], $order->permutation($order->keys($rows), 3));
    }

    public function test_strings_compare_by_bytes_never_as_numbers(): void
    {
        $rows = array_to_rows([
            ['v' => '9'],
            ['v' => '10'],
            ['v' => 'a'],
            ['v' => 'B'],
            ['v' => ''],
            ['v' => '09'],
            ['v' => '100'],
        ], schema(str_schema('v')));
        $order = new RowOrder([ref('v')->asc()]);

        static::assertSame(
            ['', '09', '10', '100', '9', 'B', 'a'],
            $rows
                ->gather($order->permutation($order->keys($rows), 7))
                ->column('v')
                ->values(),
        );
    }

    public function test_false_sorts_before_true(): void
    {
        $rows = array_to_rows([['v' => true], ['v' => false], ['v' => null]], schema(bool_schema('v', true)));
        $order = new RowOrder([ref('v')->asc()]);

        static::assertSame([2, 1, 0], $order->permutation($order->keys($rows), 3));
    }

    public function test_datetimes_sort_by_instant_whatever_their_zone(): void
    {
        $rows = array_to_rows([
            ['v' => new DateTimeImmutable('2026-01-01 02:00:00', new DateTimeZone('Europe/Warsaw'))],
            ['v' => new DateTimeImmutable('2026-01-01 00:30:00', new DateTimeZone('UTC'))],
            ['v' => new DateTimeImmutable('2026-01-01 01:00:00', new DateTimeZone('UTC'))],
        ], schema(datetime_schema('v')));
        $order = new RowOrder([ref('v')->asc()]);

        static::assertSame([1, 0, 2], $order->permutation($order->keys($rows), 3));
    }

    public function test_dates_sort_by_day_and_times_by_duration(): void
    {
        $rows = array_to_rows(
            [
                ['d' => new DateTimeImmutable('2026-03-01'), 't' => new DateInterval('PT2H')],
                ['d' => new DateTimeImmutable('1969-12-31'), 't' => new DateInterval('PT90M')],
                ['d' => new DateTimeImmutable('2026-02-28'), 't' => new DateInterval('PT1H30M1S')],
            ],
            schema(date_schema('d'), time_schema('t')),
        );
        $byDay = new RowOrder([ref('d')->asc()]);
        $byDuration = new RowOrder([ref('t')->asc()]);

        static::assertSame([1, 2, 0], $byDay->permutation($byDay->keys($rows), 3));
        static::assertSame([1, 2, 0], $byDuration->permutation($byDuration->keys($rows), 3));
    }

    public function test_later_keys_break_ties_in_their_own_direction(): void
    {
        $rows = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y'], ['a' => 1, 'b' => 'y'], ['a' => 2, 'b' => 'x']],
            schema(int_schema('a'), str_schema('b')),
        );
        $order = new RowOrder([ref('a')->desc(), ref('b')->asc()]);

        static::assertSame([3, 1, 0, 2], $order->permutation($order->keys($rows), 4));
    }

    public function test_rows_equal_on_every_key_keep_their_input_order(): void
    {
        $rows = array_to_rows([['v' => 1], ['v' => 1], ['v' => 0], ['v' => 1]], schema(int_schema('v')));
        $order = new RowOrder([ref('v')->desc()]);

        static::assertSame([0, 1, 3, 2], $order->permutation($order->keys($rows), 4));
    }

    public function test_a_batch_of_fewer_than_two_rows_keeps_its_order(): void
    {
        $order = new RowOrder([ref('v')->asc()]);

        static::assertSame([], $order->permutation($order->keys(array_to_rows([], schema(int_schema('v')))), 0));
        static::assertSame(
            [0],
            $order->permutation($order->keys(array_to_rows([['v' => 1]], schema(int_schema('v')))), 1),
        );
    }

    public function test_first_after_finds_the_first_row_past_the_bound(): void
    {
        $rows = array_to_rows([['v' => 1], ['v' => 2], ['v' => 2], ['v' => 3]], schema(int_schema('v')));
        $bound = array_to_rows([['v' => 2]], schema(int_schema('v')));
        $order = new RowOrder([ref('v')->asc()]);
        $keys = $order->keys($rows);

        static::assertSame(3, $order->firstAfter($keys, 0, 4, $order->keys($bound), 0, true));
        static::assertSame(1, $order->firstAfter($keys, 0, 4, $order->keys($bound), 0, false));
        static::assertSame(3, $order->firstAfter($keys, 3, 4, $order->keys($bound), 0, false));
    }

    public function test_compare_agrees_with_the_permutation(): void
    {
        for ($seed = 1; $seed <= 50; $seed++) {
            $dataset = SortDatasetMother::random($seed);
            $rows = array_to_rows(array_merge(...$dataset['runs']), $dataset['schema']);
            $order = new RowOrder($dataset['columns']);
            $keys = $order->keys($rows);
            $permutation = $order->permutation($keys, $rows->count());

            for ($i = 1; $i < $rows->count(); $i++) {
                $comparison = $order->compare($keys, $permutation[$i - 1], $keys, $permutation[$i]);

                static::assertTrue(
                    $comparison < 0 || $comparison === 0 && $permutation[$i - 1] < $permutation[$i],
                    "seed {$seed}, position {$i}",
                );
            }
        }
    }

    public function test_the_explicit_default_placement_orders_like_the_implicit_one(): void
    {
        $rows = array_to_rows([
            ['v' => 1.0],
            ['v' => null],
            ['v' => NAN],
            ['v' => -1.0],
        ], schema(float_schema('v', true)));

        foreach ([
            [ref('v')->asc(NullsOrder::FIRST), ref('v')->asc()],
            [ref('v')->desc(NullsOrder::LAST), ref('v')->desc()],
        ] as [$explicit, $implicit]) {
            $explicitOrder = new RowOrder([$explicit]);
            $implicitOrder = new RowOrder([$implicit]);

            static::assertSame(
                $implicitOrder->permutation($implicitOrder->keys($rows), 4),
                $explicitOrder->permutation($explicitOrder->keys($rows), 4),
            );
        }

        $ascending = new RowOrder([ref('v')->asc(NullsOrder::FIRST)]);
        $descending = new RowOrder([ref('v')->desc(NullsOrder::LAST)]);

        static::assertSame([1, 3, 0, 2], $ascending->permutation($ascending->keys($rows), 4));
        static::assertSame([2, 0, 3, 1], $descending->permutation($descending->keys($rows), 4));
    }

    public function test_a_reference_moves_nulls_to_the_other_end(): void
    {
        $rows = array_to_rows([
            ['v' => 1.0],
            ['v' => null],
            ['v' => NAN],
            ['v' => -1.0],
        ], schema(float_schema('v', true)));
        $ascendingLast = new RowOrder([ref('v')->asc(NullsOrder::LAST)]);
        $descendingFirst = new RowOrder([ref('v')->desc(NullsOrder::FIRST)]);

        static::assertSame([3, 0, 2, 1], $ascendingLast->permutation($ascendingLast->keys($rows), 4));
        static::assertSame([1, 2, 0, 3], $descendingFirst->permutation($descendingFirst->keys($rows), 4));
    }

    public function test_a_key_the_batch_lacks_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "c" does not exist');

        (new RowOrder([ref('c')->asc()]))->keys(array_to_rows([['a' => 1]], schema(int_schema('a'))));
    }

    public function test_rows_tie_when_every_column_holds_an_equal_value(): void
    {
        static::assertSame(
            [false, true, false],
            (new RowOrder([ref('at')->asc(), ref('n')->asc()]))->ties(array_to_rows(
                [
                    ['at' => new DateTimeImmutable('2024-01-01 01:00:00', new DateTimeZone('+01:00')), 'n' => 1],
                    ['at' => new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('UTC')), 'n' => 1],
                    ['at' => new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('UTC')), 'n' => 2],
                ],
                schema(datetime_schema('at'), int_schema('n')),
            )),
        );
    }

    public function test_nan_ties_with_nan(): void
    {
        static::assertSame(
            [false, true, false],
            (new RowOrder([ref('v')->asc()]))->ties(array_to_rows([
                ['v' => NAN],
                ['v' => NAN],
                ['v' => 1.0],
            ], schema(float_schema('v')))),
        );
    }

    public function test_an_empty_partition_has_no_ties(): void
    {
        static::assertSame([], (new RowOrder([ref('v')->asc()]))->ties(array_to_rows([], schema(float_schema('v')))));
    }

    public function test_lists_tie_by_their_values(): void
    {
        static::assertSame(
            [false, true, false],
            (new RowOrder([ref('l')->asc()]))->ties(array_to_rows([
                ['l' => [1, 2]],
                ['l' => [1, 2]],
                ['l' => [2]],
            ], schema(list_schema('l', type_list(type_integer()))))),
        );
    }

    public function test_lists_of_one_element_type_tie_only_when_equal(): void
    {
        static::assertSame(
            [false, true, false],
            (new RowOrder([ref('l')->asc()]))->ties(array_to_rows([
                ['l' => [1]],
                ['l' => [1]],
                ['l' => [2]],
            ], schema(list_schema('l', type_list(type_integer()))))),
        );
    }

    public function test_xml_elements_of_different_documents_tie_by_their_markup(): void
    {
        static::assertSame(
            [false, true, false],
            (new RowOrder([ref('e')->asc()]))->ties(array_to_rows([
                ['e' => type_xml_element()->cast('<div><p>a</p></div>')->firstElementChild],
                ['e' => type_xml_element()->cast('<section><p>a</p></section>')->firstElementChild],
                ['e' => type_xml_element()->cast('<div><p>b</p></div>')->firstElementChild],
            ], schema(xml_element_schema('e')))),
        );
    }

    public function test_uuids_sort_by_their_bytes(): void
    {
        $order = new RowOrder([ref('u')->asc()]);

        static::assertSame(
            [1, 2, 0],
            $order->permutation(
                $order->keys(array_to_rows([
                    ['u' => new Uuid('ffffffff-0000-4000-8000-000000000000')],
                    ['u' => new Uuid('00000000-0000-4000-8000-000000000000')],
                    ['u' => new Uuid('88888888-0000-4000-8000-000000000000')],
                ], schema(uuid_schema('u')))),
                3,
            ),
        );
    }
}
