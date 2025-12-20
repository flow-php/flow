<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\typed;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;

final class DateTimeConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{\DateTimeImmutable, string}>
     */
    public static function provide_datetime_objects() : \Generator
    {
        yield 'datetime immutable' => [
            new \DateTimeImmutable('2024-03-15 14:30:00'),
            '2024-03-15 14:30:00',
        ];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_timestamp_values() : \Generator
    {
        yield 'standard timestamp' => ['2024-03-15 14:30:00', '2024-03-15 14:30:00'];
        yield 'midnight' => ['2024-03-15 00:00:00', '2024-03-15 00:00:00'];
        yield 'end of day' => ['2024-03-15 23:59:59', '2024-03-15 23:59:59'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_timestamp_with_microseconds() : \Generator
    {
        yield 'with microseconds' => ['2024-03-15 14:30:00.123456', '2024-03-15 14:30:00.123456'];
        yield 'milliseconds only' => ['2024-03-15 14:30:00.123', '2024-03-15 14:30:00.123'];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_timestamptz_values() : \Generator
    {
        yield 'utc timestamp' => ['2024-03-15 14:30:00+00'];
        yield 'positive offset' => ['2024-03-15 16:30:00+02'];
        yield 'negative offset' => ['2024-03-15 09:30:00-05'];
    }

    #[DataProvider('provide_datetime_objects')]
    public function test_datetime_object_to_timestamp(\DateTimeImmutable $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::timestamp AS val', [typed($input, PostgreSqlType::TIMESTAMP)]);

        self::assertIsString($result);
        self::assertSame($expected, $result);
    }

    public function test_null_timestamp() : void
    {
        $result = $this->fetchValue('SELECT NULL::timestamp AS val');

        self::assertNull($result);
    }

    public function test_null_timestamptz() : void
    {
        $result = $this->fetchValue('SELECT NULL::timestamptz AS val');

        self::assertNull($result);
    }

    #[DataProvider('provide_timestamp_values')]
    public function test_timestamp_round_trip(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::timestamp AS val', [$input]);

        self::assertIsString($result);
        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_timestamp_with_microseconds')]
    public function test_timestamp_with_microseconds(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::timestamp AS val', [$input]);

        self::assertIsString($result);
        self::assertStringStartsWith($expected, $result);
    }

    #[DataProvider('provide_timestamptz_values')]
    public function test_timestamptz_round_trip(string $input) : void
    {
        $result = $this->fetchValue('SELECT $1::timestamptz AS val', [$input]);

        self::assertIsString($result);
    }
}
