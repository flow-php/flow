<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml_element;

final class IsInTest extends FlowTestCase
{
    public function test_is_in_accepts_a_container_haystack_without_a_single_element_type(): void
    {
        // Parameter::asArray() iterates a structure, a map and a bare array alike, so the bind gate
        // must not refuse them - only a list and a map declare one element type to compare against.
        static::assertSame(
            'boolean',
            lit('a')
                ->isIn(lit([1, 'a']))
                ->returns()
                ->toString(),
        );
        static::assertTrue((new FunctionContext(flow_context()))->eval(lit('a')->isIn(lit([1, 'a'])), [], schema()));
    }

    public function test_is_in_matches_two_equal_datetimes(): void
    {
        // in_array(strict: true) compared these by identity and answered false
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            lit(new DateTimeImmutable('2024-01-01 10:00:00'))
                ->isIn(lit([new DateTimeImmutable('2024-01-01 10:00:00')])),
            [],
            schema(),
        ));
    }

    public function test_is_in_matches_two_equal_date_intervals_by_value(): void
    {
        // a time list of the needle's type compares by the physical form, as equals() does
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            lit(new DateInterval('PT1H'))->equals(lit(new DateInterval('PT60M'))),
            [],
            schema(),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            lit(new DateInterval('PT1H'))->isIn(lit([new DateInterval('PT60M')])),
            [],
            schema(),
        ));
    }

    public function test_is_in_over_an_empty_haystack(): void
    {
        static::assertSame('boolean', lit('a')->isIn(lit([]))->returns()->toString());
        static::assertFalse((new FunctionContext(flow_context()))->eval(lit('a')->isIn(lit([])), [], schema()));
    }

    public function test_is_in_agrees_with_equals_on_numeric_operands(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(1)),
            ['a' => '1'],
            schema(str_schema('a')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->isIn(lit([1])),
            ['a' => '1'],
            schema(str_schema('a')),
        ));
    }

    public function test_is_in_refuses_an_incomparable_element_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "Can't compare '(string == date)' due to data type mismatch - an explicit cast is required.",
        );

        lit('a')->isIn(lit([new DateTimeImmutable('2024-01-01')]))->returns();
    }

    public function test_a_match_beats_a_null_element(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(lit(5)->isIn(lit([1, null, 5])), [], schema()));
    }

    public function test_no_match_with_a_null_element_is_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(lit(7)->isIn(lit([1, null, 5])), [], schema()));
    }

    public function test_no_match_without_a_null_element_is_false(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(lit(7)->isIn(lit([1, 3, 5])), [], schema()));
    }

    public function test_a_null_needle_is_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(lit(null)->isIn(lit([1, 3, 5])), [], schema()));
    }

    public function test_a_null_haystack_is_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(lit(5)->isIn(lit(null)), [], schema()));
    }

    /**
     * @return Generator<string, array{Type<mixed>, mixed, mixed, bool}>
     */
    public static function values_of_one_type(): Generator
    {
        yield 'uuid' => [
            type_uuid(),
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000001',
            true,
        ];
        yield 'other uuid' => [
            type_uuid(),
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000002',
            false,
        ];
        yield 'json' => [type_json(), '{"a":1,"b":2}', '{"a":1,"b":2}', true];
        yield 'json compares its stored text' => [type_json(), '{"a":1,"b":2}', '{"b":2,"a":1}', false];
        yield 'enum' => [type_enum(BackedStringEnum::class), BackedStringEnum::one, BackedStringEnum::one, true];
        yield 'datetime of the same instant in another zone' => [
            type_datetime(),
            new DateTimeImmutable('2024-01-01 12:00:00', new DateTimeZone('UTC')),
            new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('Europe/Warsaw')),
            true,
        ];
        yield 'date' => [type_date(), new DateTimeImmutable('2024-01-01'), new DateTimeImmutable('2024-01-01'), true];
        yield 'time' => [type_time(), new DateInterval('PT60M'), new DateInterval('PT1H'), true];
        yield 'xml elements of different documents' => [
            type_xml_element(),
            type_xml_element()->cast('<div><p>a</p></div>')->firstElementChild,
            type_xml_element()->cast('<section><p>a</p></section>')->firstElementChild,
            true,
        ];
        yield 'other xml elements' => [
            type_xml_element(),
            type_xml_element()->cast('<div><p>a</p></div>')->firstElementChild,
            type_xml_element()->cast('<div><p>b</p></div>')->firstElementChild,
            false,
        ];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('values_of_one_type')]
    public function test_a_list_of_the_needle_type_matches_by_value(
        Type $type,
        mixed $needle,
        mixed $element,
        bool $in,
    ): void {
        static::assertSame($in, (new FunctionContext(flow_context()))->eval(
            ref('a')->isIn(ref('h')),
            ['a' => $needle, 'h' => [$element]],
            schema(definition_from_type('a', $type), definition_from_type('h', type_list($type))),
        ));
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('values_of_one_type')]
    public function test_a_map_of_the_needle_type_matches_by_value(
        Type $type,
        mixed $needle,
        mixed $element,
        bool $in,
    ): void {
        static::assertSame($in, (new FunctionContext(flow_context()))->eval(
            ref('a')->isIn(ref('h')),
            ['a' => $needle, 'h' => ['k' => $element]],
            schema(definition_from_type('a', $type), definition_from_type('h', type_map(type_string(), $type))),
        ));
    }
}
