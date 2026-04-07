<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_date, literal, param, select, typed};
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class DateConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_date_values() : \Generator
    {
        yield 'standard date' => ['2024-03-15', '2024-03-15'];
        yield 'year start' => ['2024-01-01', '2024-01-01'];
        yield 'year end' => ['2024-12-31', '2024-12-31'];
        yield 'leap year' => ['2024-02-29', '2024-02-29'];
        yield 'far future' => ['2100-06-15', '2100-06-15'];
        yield 'past date' => ['1990-05-20', '1990-05-20'];
    }

    /**
     * @return \Generator<string, array{\DateTimeImmutable, string}>
     */
    public static function provide_datetime_to_date() : \Generator
    {
        yield 'datetime immutable' => [
            new \DateTimeImmutable('2024-03-15 14:30:00'),
            '2024-03-15',
        ];
        yield 'with timezone' => [
            new \DateTimeImmutable('2024-06-20 23:59:59', new \DateTimeZone('America/New_York')),
            '2024-06-20',
        ];
    }

    #[DataProvider('provide_date_values')]
    public function test_date_round_trip(string $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_date())->as('val'))->toSql(), [$input]);

        self::assertIsString($result);
        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_datetime_to_date')]
    public function test_datetime_object_to_date(\DateTimeImmutable $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_date())->as('val'))->toSql(), [typed($input, ValueType::DATE)]);

        self::assertIsString($result);
        self::assertSame($expected, $result);
    }

    public function test_null_date() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_date())->as('val'))->toSql());

        self::assertNull($result);
    }
}
