<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_custom, column_type_time, literal, param, select};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class TimeConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_time_values() : \Generator
    {
        yield 'standard time' => ['14:30:00', '14:30:00'];
        yield 'midnight' => ['00:00:00', '00:00:00'];
        yield 'end of day' => ['23:59:59', '23:59:59'];
        yield 'noon' => ['12:00:00', '12:00:00'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_time_with_microseconds() : \Generator
    {
        yield 'with microseconds' => ['14:30:00.123456', '14:30:00.123456'];
        yield 'milliseconds only' => ['14:30:00.123', '14:30:00.123'];
    }

    /**
     * @return \Generator<string, array{string, string, string}>
     */
    public static function provide_timetz_values() : \Generator
    {
        yield 'utc time' => ['14:30:00+00', '14:30:00', '+00:00'];
        yield 'positive offset' => ['14:30:00+02', '14:30:00', '+02:00'];
        yield 'negative offset' => ['14:30:00-05', '14:30:00', '-05:00'];
    }

    public function test_null_time() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_time())->as('val'))->toSql());

        self::assertNull($result);
    }

    #[DataProvider('provide_time_values')]
    public function test_time_round_trip(string $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_time())->as('val'))->toSql(), [$input]);

        self::assertIsString($result);
        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_time_with_microseconds')]
    public function test_time_with_microseconds(string $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_time())->as('val'))->toSql(), [$input]);

        self::assertIsString($result);
        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_timetz_values')]
    public function test_timetz_round_trip(string $input, string $expectedTime, string $expectedOffset) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_custom('timetz'))->as('val'))->toSql(), [$input]);

        self::assertIsString($result);
        self::assertStringStartsWith($expectedTime, $result);
    }
}
