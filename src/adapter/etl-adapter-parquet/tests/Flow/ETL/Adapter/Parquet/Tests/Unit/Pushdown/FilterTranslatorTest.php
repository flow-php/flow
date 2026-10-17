<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\Pushdown;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\Pushdown\FilterTranslator;
use Flow\ETL\Adapter\Parquet\Pushdown\TranslatedConjunct;
use Flow\ETL\Adapter\Parquet\Tests\Mother\BodySchemaMother;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Filter\Filter;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_any;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;

use const NAN;

final class FilterTranslatorTest extends FlowTestCase
{
    public static function translated(): Generator
    {
        yield 'equals' => [ref('id')->equals(lit(5)), parquet_column('id')->equals(5)];
        yield 'not equals' => [ref('id')->notEquals(lit(5)), parquet_column('id')->notEquals(5)];
        yield 'less than' => [ref('id')->lessThan(lit(5)), parquet_column('id')->lessThan(5)];
        yield 'less than or equal' => [ref('id')->lessThanEqual(lit(5)), parquet_column('id')->lessThanOrEqual(5)];
        yield 'greater than' => [ref('id')->greaterThan(lit(5)), parquet_column('id')->greaterThan(5)];
        yield 'greater than or equal' => [
            ref('id')->greaterThanEqual(lit(5)),
            parquet_column('id')->greaterThanOrEqual(5),
        ];
        yield 'a literal on the left is flipped' => [lit(5)->lessThan(ref('id')), parquet_column('id')->greaterThan(5)];
        yield 'a literal on the left of an equality' => [lit(5)->equals(ref('id')), parquet_column('id')->equals(5)];
        yield 'same on the same type' => [ref('id')->same(lit(5)), parquet_column('id')->equals(5)];
        yield 'not same on the same type' => [ref('id')->notSame(lit(5)), parquet_column('id')->notEquals(5)];
        yield 'is true' => [ref('flag')->isTrue(), parquet_column('flag')->equals(true)];
        yield 'is null' => [ref('id')->isNull(), parquet_column('id')->isNull()];
        yield 'is not null' => [ref('id')->isNotNull(), parquet_column('id')->isNotNull()];
        yield 'not' => [not(ref('id')->equals(lit(5))), parquet_not(parquet_column('id')->equals(5))];
        yield 'an or' => [
            ref('id')->equals(lit(5))->or(ref('name')->equals(lit('a'))),
            parquet_any(parquet_column('id')->equals(5), parquet_column('name')->equals('a')),
        ];
        yield 'an and inside an or' => [
            ref('id')->equals(lit(5))->or(ref('id')->greaterThan(lit(1))->and(ref('name')->equals(lit('a')))),
            parquet_any(
                parquet_column('id')->equals(5),
                parquet_all(parquet_column('id')->greaterThan(1), parquet_column('name')->equals('a')),
            ),
        ];
        yield 'is in' => [ref('id')->isIn(lit([1, 2])), parquet_column('id')->in([1, 2])];
        yield 'is in drops null elements' => [ref('id')->isIn(lit([1, null, 2])), parquet_column('id')->in([1, 2])];
        yield 'a negated is in without null' => [
            not(ref('id')->isIn(lit([1, 2]))),
            parquet_not(parquet_column('id')->in([1, 2])),
        ];
        yield 'a negated is in holding null drops the null' => [
            not(ref('id')->isIn(lit([1, null]))),
            parquet_not(parquet_column('id')->in([1])),
        ];
        yield 'starts with' => [ref('name')->startsWith(lit('ab')), parquet_column('name')->startsWith('ab')];
        yield 'between left inclusive' => [
            ref('id')->between(lit(1), lit(5)),
            parquet_all(parquet_column('id')->greaterThanOrEqual(1), parquet_column('id')->lessThan(5)),
        ];
        yield 'between inclusive' => [
            ref('id')->between(lit(1), lit(5), lit(Boundary::INCLUSIVE)),
            parquet_all(parquet_column('id')->greaterThanOrEqual(1), parquet_column('id')->lessThanOrEqual(5)),
        ];
        yield 'between right inclusive' => [
            ref('id')->between(lit(1), lit(5), lit(Boundary::RIGHT_INCLUSIVE)),
            parquet_all(parquet_column('id')->greaterThan(1), parquet_column('id')->lessThanOrEqual(5)),
        ];
        yield 'between exclusive' => [
            ref('id')->between(lit(1), lit(5), lit(Boundary::EXCLUSIVE)),
            parquet_all(parquet_column('id')->greaterThan(1), parquet_column('id')->lessThan(5)),
        ];
        yield 'an int literal on a float column becomes a float' => [
            ref('price')->greaterThan(lit(10)),
            parquet_column('price')->greaterThan(10.0),
        ];
        yield 'an int literal on a float column at 2^53' => [
            ref('price')->equals(lit(9_007_199_254_740_992)),
            parquet_column('price')->equals(9_007_199_254_740_992.0),
        ];
        yield 'an integral float literal on an int column becomes an int' => [
            ref('id')->lessThan(lit(5.0)),
            parquet_column('id')->lessThan(5),
        ];
        yield 'a float literal on a float column' => [
            ref('price')->lessThan(lit(1.5)),
            parquet_column('price')->lessThan(1.5),
        ];
        yield 'strings' => [ref('name')->greaterThan(lit('b')), parquet_column('name')->greaterThan('b')];
        yield 'a canonical digit string on a numeric string column' => [
            ref('big')->greaterThan(lit('18446744073709551614')),
            parquet_column('big')->greaterThan('18446744073709551614'),
        ];
        yield 'zero on a numeric string column' => [ref('big')->equals(lit('0')), parquet_column('big')->equals('0')];
        yield 'a uuid literal becomes its string form' => [
            ref('uuid')->equals(lit(new Uuid('6c2f6e9e-4ec0-4ef7-9b2e-7f2d3f2f6b11'))),
            parquet_column('uuid')->equals('6c2f6e9e-4ec0-4ef7-9b2e-7f2d3f2f6b11'),
        ];
        yield 'a datetime literal passes through' => [
            ref('ts')->greaterThan(lit(new DateTimeImmutable('2024-01-01 12:00:00'))),
            parquet_column('ts')->greaterThan(new DateTimeImmutable('2024-01-01 12:00:00')),
        ];
        yield 'a date literal passes through' => [
            ref('day')->lessThan(lit(new DateTimeImmutable('2024-01-01'))),
            parquet_column('day')->lessThan(new DateTimeImmutable('2024-01-01')),
        ];
        yield 'a date literal on a datetime column' => [
            ref('ts')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-01'))),
            parquet_column('ts')->greaterThanOrEqual(new DateTimeImmutable('2024-01-01')),
        ];
        yield 'a datetime literal on a date column' => [
            ref('day')->lessThan(lit(new DateTimeImmutable('2024-01-01 12:00:00'))),
            parquet_column('day')->lessThan(new DateTimeImmutable('2024-01-01 12:00:00')),
        ];
        yield 'json is null' => [ref('doc')->isNull(), parquet_column('doc')->isNull()];
    }

    #[DataProvider('translated')]
    public function test_a_pushable_conjunct_is_translated(ScalarFunction $function, Filter $expected): void
    {
        $translated = (new FilterTranslator())->translate($function, BodySchemaMother::everyLeafType());

        static::assertCount(1, $translated);
        static::assertEquals($expected, $translated[0]->filter);
    }

    public static function refused(): Generator
    {
        yield 'a column outside the body' => [ref('year')->equals(lit(2024))];
        yield 'two references' => [ref('id')->equals(ref('price'))];
        yield 'two literals' => [lit(1)->equals(lit(1))];
        yield 'an unsupported function' => [ref('name')->endsWith(lit('a'))];
        yield 'an unsupported function inside a not' => [not(ref('name')->endsWith(lit('a')))];
        yield 'an unsupported function inside an or' => [ref('id')->equals(lit(5))->or(ref('name')->endsWith(lit(
            'a',
        )))];
        yield 'is in over a reference haystack' => [ref('id')->isIn(ref('ids'))];
        yield 'is in with a refused element' => [ref('id')->isIn(lit([1, 'a']))];
        yield 'starts with on an int column' => [ref('id')->startsWith(lit('1'))];
        yield 'between with a refused bound' => [ref('id')->between(lit(1), lit('5'))];
        yield 'same across int and float' => [ref('id')->same(lit(5.0))];
        yield 'same on a numeric string column' => [ref('big')->same(lit('5'))];
        yield 'an int literal on a float column above 2^53' => [ref('price')->equals(lit(9_007_199_254_740_993))];
        yield 'a fractional float literal on an int column' => [ref('id')->lessThan(lit(5.5))];
        yield 'an integral float literal on an int column at 2^53' => [ref('id')->equals(lit(9_007_199_254_740_992.0))];
        yield 'a NaN literal' => [ref('price')->equals(lit(NAN))];
        yield 'an int literal on a string column' => [ref('name')->equals(lit(5))];
        yield 'a string literal on an int column' => [ref('id')->equals(lit('5'))];
        yield 'a numeric string column with a leading zero literal' => [ref('big')->equals(lit('05'))];
        yield 'a numeric string column with a non digit literal' => [ref('big')->equals(lit('-5'))];
        yield 'a numeric string column with an int literal' => [ref('big')->equals(lit(5))];
        yield 'ordering on a uuid column' => [
            ref('uuid')->greaterThan(lit(new Uuid('6c2f6e9e-4ec0-4ef7-9b2e-7f2d3f2f6b11'))),
        ];
        yield 'equality on a json column' => [ref('doc')->equals(lit('{}'))];
        yield 'same across date and datetime' => [ref('day')->same(lit(new DateTimeImmutable('2024-01-01 12:00:00')))];
    }

    #[DataProvider('refused')]
    public function test_a_conjunct_that_cannot_be_pushed_is_refused(ScalarFunction $function): void
    {
        static::assertSame([], (new FilterTranslator())->translate($function, BodySchemaMother::everyLeafType()));
    }

    public function test_every_conjunct_of_an_and_is_translated_on_its_own(): void
    {
        $translated = (new FilterTranslator())->translate(
            ref('id')
                ->greaterThan(lit(1))
                ->and(ref('name')->endsWith(lit('a')))
                ->and(ref('name')->equals(lit('b'))->or(ref('price')->lessThan(lit(2.0)))),
            BodySchemaMother::everyLeafType(),
        );

        static::assertEquals(
            [
                new TranslatedConjunct(parquet_column('id')->greaterThan(1), ['id']),
                new TranslatedConjunct(
                    parquet_any(parquet_column('name')->equals('b'), parquet_column('price')->lessThan(2.0)),
                    ['name', 'price'],
                ),
            ],
            $translated,
        );
    }
}
