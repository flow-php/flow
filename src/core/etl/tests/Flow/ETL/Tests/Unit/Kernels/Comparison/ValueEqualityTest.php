<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Comparison;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Kernels\Comparison\ValueEquality;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\time_schema;

final class ValueEqualityTest extends FlowTestCase
{
    public function test_a_null_on_either_side_is_unknown(): void
    {
        $rows = array_to_rows(
            [['l' => null, 'r' => 1], ['l' => 1, 'r' => null], ['l' => 2, 'r' => 2]],
            schema(int_schema('l', nullable: true), int_schema('r', nullable: true)),
        );

        static::assertSame([null, null, true], (new ValueEquality())->equal($rows->column('l'), $rows->column('r')));
    }

    public function test_strings_are_equal_only_when_identical(): void
    {
        $rows = array_to_rows(
            [['l' => '1', 'r' => '1.0'], ['l' => 'a', 'r' => 'a']],
            schema(str_schema('l'), str_schema('r')),
        );

        static::assertSame([false, true], (new ValueEquality())->equal($rows->column('l'), $rows->column('r')));
    }

    public function test_an_int_and_a_float_compare_by_value(): void
    {
        $rows = array_to_rows(
            [['l' => 1, 'r' => 1.0], ['l' => 2, 'r' => 2.5]],
            schema(int_schema('l'), float_schema('r')),
        );

        static::assertSame([true, false], (new ValueEquality())->equal($rows->column('l'), $rows->column('r')));
    }

    public function test_datetimes_compare_by_instant(): void
    {
        $rows = array_to_rows(
            [[
                'l' => new DateTimeImmutable('2026-01-01T02:00:00+02:00'),
                'r' => new DateTimeImmutable('2026-01-01T00:00:00+00:00'),
            ]],
            schema(datetime_schema('l'), datetime_schema('r')),
        );

        static::assertSame([true], (new ValueEquality())->equal($rows->column('l'), $rows->column('r')));
    }

    public function test_times_compare_by_their_length(): void
    {
        $rows = array_to_rows(
            [
                ['l' => new DateInterval('P1D'), 'r' => new DateInterval('PT24H')],
                ['l' => new DateInterval('P1D'), 'r' => new DateInterval('PT23H')],
            ],
            schema(time_schema('l'), time_schema('r')),
        );

        static::assertSame([true, false], (new ValueEquality())->equal($rows->column('l'), $rows->column('r')));
    }

    public function test_in_casts_every_candidate_to_the_type_it_compares_in_with_its_needle(): void
    {
        static::assertSame(
            [true, false, true],
            (new ValueEquality())->in(
                array_to_rows([['n' => 1], ['n' => 2], ['n' => 3]], schema(int_schema('n')))->column('n'),
                [['1'], [1.0, 5], [4, '3']],
                'n',
                'h',
            ),
        );
    }

    public function test_in_is_unknown_for_a_null_needle_a_null_haystack_and_no_match_beside_a_null(): void
    {
        static::assertSame(
            [null, null, null, true],
            (new ValueEquality())->in(
                array_to_rows([
                    ['n' => null],
                    ['n' => 1],
                    ['n' => 1],
                    ['n' => 1],
                ], schema(int_schema('n', nullable: true)))->column('n'),
                [[1], null, [2, null], [null, 1]],
                'n',
                'h',
            ),
        );
    }

    public function test_in_names_the_row_of_a_candidate_that_does_not_cast(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage(
            "Column \"h\" value 'x' cannot be cast to integer to compare: \"x\" is not a number (row 1)",
        );

        (new ValueEquality())->in(
            array_to_rows([['n' => 1], ['n' => 2]], schema(int_schema('n')))->column('n'),
            [[1], ['x']],
            'n',
            'h',
        );
    }

    public function test_in_names_the_row_of_a_needle_that_does_not_cast(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage(
            "Column \"n\" value 'a' cannot be cast to integer to compare: \"a\" is not a number (row 1)",
        );

        (new ValueEquality())->in(
            array_to_rows([['n' => '1'], ['n' => 'a']], schema(str_schema('n')))->column('n'),
            [[1], [2]],
            'n',
            'h',
        );
    }
}
