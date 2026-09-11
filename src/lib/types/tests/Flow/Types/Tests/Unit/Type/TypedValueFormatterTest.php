<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\Types\Type;
use Flow\Types\Type\TypedValueFormatter;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;

final class TypedValueFormatterTest extends TestCase
{
    /**
     * @return Generator<string, array{type: Type<mixed>, value: mixed, expected: string}>
     */
    public static function format_data_provider(): Generator
    {
        yield 'float' => [
            'type' => type_float(),
            'value' => 1.5,
            'expected' => '1.500000',
        ];

        yield 'date' => [
            'type' => type_date(),
            'value' => new DateTimeImmutable('2024-01-02 00:00:00', new DateTimeZone('UTC')),
            'expected' => '2024-01-02',
        ];

        yield 'time' => [
            'type' => type_time(),
            'value' => new DateInterval('PT1H2M3S'),
            'expected' => '01:02:03',
        ];

        yield 'datetime' => [
            'type' => type_datetime(),
            'value' => new DateTimeImmutable('2024-01-02 03:04:05', new DateTimeZone('UTC')),
            'expected' => '2024-01-02T03:04:05+00:00',
        ];

        yield 'boolean true' => [
            'type' => type_boolean(),
            'value' => true,
            'expected' => 'true',
        ];

        yield 'boolean false' => [
            'type' => type_boolean(),
            'value' => false,
            'expected' => 'false',
        ];

        yield 'integer' => [
            'type' => type_integer(),
            'value' => 5,
            'expected' => '5',
        ];

        yield 'string' => [
            'type' => type_string(),
            'value' => 'abc',
            'expected' => 'abc',
        ];

        yield 'uuid' => [
            'type' => type_uuid(),
            'value' => Uuid::fromString('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
            'expected' => 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
        ];

        yield 'json' => [
            'type' => type_json(),
            'value' => Json::fromString('{"x":1}'),
            'expected' => '{"x":1}',
        ];

        yield 'list' => [
            'type' => type_list(type_integer()),
            'value' => [1, 2],
            'expected' => '[1,2]',
        ];

        yield 'time zone' => [
            'type' => type_time_zone(),
            'value' => new DateTimeZone('UTC'),
            'expected' => 'UTC',
        ];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('format_data_provider')]
    public function test_format(Type $type, mixed $value, string $expected): void
    {
        static::assertSame($expected, (new TypedValueFormatter())->format($type, $value));
    }

    public function test_format_null_is_empty_string(): void
    {
        static::assertSame('', (new TypedValueFormatter())->format(type_float(), null));
        static::assertSame('', (new TypedValueFormatter())->format(type_date(), null));
        static::assertSame('', (new TypedValueFormatter())->format(type_string(), null));
    }

    public function test_format_time_interval(): void
    {
        static::assertSame('25:02:03', (new TypedValueFormatter())->format(
            type_time(),
            new DateInterval('P1DT1H2M3S'),
        ));
    }

    public function test_format_time_interval_with_microseconds(): void
    {
        static::assertSame('01:02:03.500000', (new TypedValueFormatter())->format(
            type_time(),
            (new DateTimeImmutable('2024-01-01 00:00:00.000000'))->diff(
                new DateTimeImmutable('2024-01-01 01:02:03.500000'),
            ),
        ));
    }

    public function test_format_union_falls_back_to_string_when_no_member_matches(): void
    {
        static::assertSame('abc', (new TypedValueFormatter())->format(type_union(type_float(), type_integer()), 'abc'));
    }

    public function test_format_union_uses_matching_member(): void
    {
        $formatter = new TypedValueFormatter();

        static::assertSame('1.500000', $formatter->format(type_union(type_string(), type_float()), 1.5));
        static::assertSame('1.5', $formatter->format(type_union(type_string(), type_float()), '1.5'));
    }
}
