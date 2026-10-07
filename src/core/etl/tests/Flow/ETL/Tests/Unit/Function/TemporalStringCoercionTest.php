<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\ComparisonFunction;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\TemporalStringCoercion;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_instance_of;

final class TemporalStringCoercionTest extends FlowTestCase
{
    public static function coerced(): Generator
    {
        $ts = ref('ts')->resolve(datetime_schema('ts'));

        yield 'string literal on the right' => [$ts->equals(lit('2026-10-02')), ['datetime', 'datetime']];
        yield 'string literal on the left' => [lit('2026-10-02')->lessThan($ts), ['datetime', 'datetime']];
        yield 'date column' => [ref('d')->resolve(date_schema('d'))->equals(lit('2026-10-02')), ['date', 'date']];
        yield 'nullable string column' => [
            $ts->equals(ref('n')->resolve(str_schema('n', nullable: true))),
            ['datetime', '?datetime'],
        ];
        yield 'between bounds' => [
            $ts->between(lit('2026-10-02'), lit('2026-10-03'), Boundary::INCLUSIVE),
            ['datetime', 'datetime', 'datetime'],
        ];
        yield 'is in a list of strings' => [$ts->isIn(lit(['2026-10-02'])), ['list<datetime>', 'datetime']];
        yield 'string is in a list of dates' => [
            lit('2026-10-02')->isIn(lit([new DateTimeImmutable('2026-10-02')])),
            ['list<date>', 'date'],
        ];
    }

    public static function untouched(): Generator
    {
        $s = ref('s')->resolve(str_schema('s'));

        yield 'two strings' => [$s->equals(lit('2026-10-02'))];
        yield 'integer and string' => [ref('i')->resolve(int_schema('i'))->equals(lit('1'))];
        yield 'same stays strict' => [ref('ts')->resolve(datetime_schema('ts'))->same(lit('2026-10-02'))];
        yield 'unresolved operand' => [ref('ts')->equals(lit('2026-10-02'))];
        yield 'operand without a derivable schema' => [
            array_get($s, 'key')->equals(ref('ts')->resolve(datetime_schema('ts'))),
        ];
        yield 'not a comparison' => [ref('ts')->resolve(datetime_schema('ts'))->isNull()];
    }

    /**
     * @param list<string> $expected
     */
    #[DataProvider('coerced')]
    public function test_a_string_operand_is_cast_to_the_temporal_operand(
        ScalarFunction $predicate,
        array $expected,
    ): void {
        static::assertSame($expected, array_map(static fn(ScalarFunction $operand): string => $operand
            ->returns()
            ->toString(), type_instance_of(ComparisonFunction::class)->assert((new TemporalStringCoercion())->coerce($predicate))->operands()));
    }

    #[DataProvider('untouched')]
    public function test_the_function_is_returned_as_it_is(ScalarFunction $predicate): void
    {
        static::assertSame($predicate, (new TemporalStringCoercion())->coerce($predicate));
    }
}
