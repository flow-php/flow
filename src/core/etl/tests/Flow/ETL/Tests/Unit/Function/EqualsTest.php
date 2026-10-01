<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException as TypesInvalidArgumentException;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml_element;

final class EqualsTest extends FlowTestCase
{
    public function test_null_operand_yields_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(1)),
            ['a' => null],
            schema(str_schema('a', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(ref('b')),
            ['a' => null, 'b' => null],
            schema(str_schema('a', nullable: true), str_schema('b', nullable: true)),
        ));
    }

    public function test_equal_values(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(1)),
            ['a' => 1],
            schema(int_schema('a')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit('x')),
            ['a' => 'x'],
            schema(str_schema('a')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(2)),
            ['a' => 1],
            schema(int_schema('a')),
        ));
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
    public function test_values_of_one_type_compare_by_value(Type $type, mixed $left, mixed $right, bool $equal): void
    {
        static::assertSame($equal, (new FunctionContext(flow_context()))->eval(
            ref('a')->equals(ref('b')),
            ['a' => $left, 'b' => $right],
            schema(definition_from_type('a', $type), definition_from_type('b', $type)),
        ));
    }

    public function test_a_datetime_equals_a_literal_of_the_same_instant_in_another_zone(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('Europe/Warsaw')))),
            ['a' => new DateTimeImmutable('2024-01-01 12:00:00', new DateTimeZone('UTC'))],
            schema(definition_from_type('a', type_datetime())),
        ));
    }

    public function test_a_uuid_is_not_comparable_with_a_string(): void
    {
        $this->expectException(TypesInvalidArgumentException::class);
        $this->expectExceptionMessage("Can't compare '(uuid == string)' due to data type mismatch");

        (new ReferenceResolver())
            ->resolve(
                ref('u')->equals(lit('00000000-0000-4000-8000-000000000001')),
                schema(definition_from_type('u', type_uuid())),
            )
            ->returns();
    }
}
