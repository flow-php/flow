<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\Explain\Expression;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use stdClass;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\any;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;

final class ExpressionTest extends FlowTestCase
{
    public static function operators(): Generator
    {
        yield 'equals' => [ref('a')->equals(lit(1)), 'a = 1'];
        yield 'not equals' => [ref('a')->notEquals(lit(1)), 'a != 1'];
        yield 'same' => [ref('a')->same(lit(1)), 'a === 1'];
        yield 'not same' => [ref('a')->notSame(lit(1)), 'a !== 1'];
        yield 'greater than' => [ref('a')->greaterThan(lit(1)), 'a > 1'];
        yield 'greater than or equal' => [ref('a')->greaterThanEqual(lit(1)), 'a >= 1'];
        yield 'less than' => [ref('a')->lessThan(lit(1)), 'a < 1'];
        yield 'less than or equal' => [ref('a')->lessThanEqual(lit(1)), 'a <= 1'];
        yield 'is in' => [ref('country')->isIn(lit(['PL', 'DE'])), "country IN ('PL', 'DE')"];
        yield 'is in a column' => [ref('country')->isIn(ref('countries')), 'country IN countries'];
        yield 'starts with' => [ref('name')->startsWith('ab'), "name STARTS WITH 'ab'"];
        yield 'is null' => [ref('a')->isNull(), 'a IS NULL'];
        yield 'is not null' => [ref('a')->isNotNull(), 'a IS NOT NULL'];
        yield 'and' => [all(ref('a')->isNull(), ref('b')->isNull()), 'a IS NULL AND b IS NULL'];
        yield 'or' => [any(ref('a')->isNull(), ref('b')->isNull()), 'a IS NULL OR b IS NULL'];
        yield 'not a column' => [not(ref('active')), 'NOT active'];
        yield 'not a comparison' => [not(ref('a')->equals(lit(1))), 'NOT (a = 1)'];
    }

    public static function betweens(): Generator
    {
        yield 'inclusive' => [Boundary::INCLUSIVE, 'total BETWEEN 1 AND 10'];
        yield 'left inclusive' => [Boundary::LEFT_INCLUSIVE, '1 <= total < 10'];
        yield 'right inclusive' => [Boundary::RIGHT_INCLUSIVE, '1 < total <= 10'];
        yield 'exclusive' => [Boundary::EXCLUSIVE, '1 < total < 10'];
    }

    public static function values(): Generator
    {
        yield 'null' => [null, 'null'];
        yield 'true' => [true, 'true'];
        yield 'false' => [false, 'false'];
        yield 'integer' => [12, '12'];
        yield 'float keeps its fraction' => [1.0, '1.0'];
        yield 'string is quoted, quotes doubled' => ["it's", "'it''s'"];
        yield 'control characters are escaped' => ["first\nsecond\t", "'first\\nsecond\\t'"];
        yield 'date is ISO 8601' => [
            new DateTimeImmutable('2026-10-20 00:00:00', new DateTimeZone('UTC')),
            '2026-10-20T00:00:00+00:00',
        ];
        yield 'enum is its case' => [Boundary::INCLUSIVE, 'Boundary::INCLUSIVE'];
        yield 'any other object is its name' => [new stdClass(), 'stdClass'];
        yield 'list in parentheses' => [[1, 2, 3, 4, 5], '(1, 2, 3, 4, 5)'];
        yield 'nested list' => [[[1, 2], [3]], '((1, 2), (3))'];
        yield 'long list is cut' => [
            ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'],
            "('a', 'b', 'c', 'd', 'e', … 7 more)",
        ];
    }

    #[DataProvider('operators')]
    public function test_an_operator_reads_as_its_operands_around_it(ScalarFunction $function, string $expected): void
    {
        static::assertSame($expected, (new Expression())->of($function));
    }

    #[DataProvider('betweens')]
    public function test_between_reads_as_its_boundary(Boundary $boundary, string $expected): void
    {
        static::assertSame($expected, (new Expression())->of(ref('total')->between(lit(1), lit(10), $boundary)));
    }

    #[DataProvider('values')]
    public function test_a_literal_reads_as_its_value(mixed $value, string $expected): void
    {
        static::assertSame($expected, (new Expression())->of(lit($value)));
    }

    public function test_a_nested_and_or_is_parenthesized(): void
    {
        static::assertSame(
            'a = 1 AND (b = 2 OR c = 3)',
            (new Expression())->of(all(
                ref('a')->equals(lit(1)),
                any(ref('b')->equals(lit(2)), ref('c')->equals(lit(3))),
            )),
        );
    }

    public function test_a_not_or_comparison_inside_an_and_is_not_parenthesized(): void
    {
        static::assertSame(
            'NOT active AND a = 1',
            (new Expression())->of(all(not(ref('active')), ref('a')->equals(lit(1)))),
        );
    }

    public function test_an_operator_compared_is_parenthesized(): void
    {
        static::assertSame('(a = 1) = true', (new Expression())->of(ref('a')->equals(lit(1))->equals(lit(true))));
    }

    public function test_a_function_without_wording_reads_as_its_name_over_its_arguments(): void
    {
        static::assertSame("ToUpper(name) = 'X'", (new Expression())->of(ref('name')->upper()->equals(lit('X'))));
    }

    public function test_between_with_a_literal_that_is_not_a_boundary_reads_as_its_name(): void
    {
        static::assertSame(
            "Between(total, 1, 10, 'x')",
            (new Expression())->of(ref('total')->between(lit(1), lit(10), lit('x'))),
        );
    }

    public function test_between_with_a_boundary_known_only_at_run_time_reads_as_its_name(): void
    {
        static::assertSame(
            'Between(total, 1, 10, boundary)',
            (new Expression())->of(ref('total')->between(lit(1), lit(10), ref('boundary'))),
        );
    }
}
