<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use DateTimeImmutable;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_timestamp;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\typed;

final class TimestampConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{\DateTimeImmutable, string}>
     */
    public static function provide_datetime_objects(): Generator
    {
        yield 'utc stays utc' => [
            new DateTimeImmutable('2024-03-15 14:30:00+00:00'),
            '2024-03-15 14:30:00+00:00',
        ];
        yield 'offset normalized to utc' => [
            new DateTimeImmutable('2024-03-15 16:30:00+02:00'),
            '2024-03-15 14:30:00+00:00',
        ];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_timestamp_values(): Generator
    {
        yield 'standard timestamp' => ['2024-03-15 14:30:00', '2024-03-15 14:30:00+00:00'];
        yield 'midnight' => ['2024-03-15 00:00:00', '2024-03-15 00:00:00+00:00'];
        yield 'end of day' => ['2024-03-15 23:59:59', '2024-03-15 23:59:59+00:00'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_timestamp_with_microseconds(): Generator
    {
        yield 'with microseconds' => ['2024-03-15 14:30:00.123456', '2024-03-15 14:30:00.123456'];
        yield 'milliseconds only' => ['2024-03-15 14:30:00.123', '2024-03-15 14:30:00.123'];
    }

    #[DataProvider('provide_datetime_objects')]
    public function test_datetime_object_to_timestamp(DateTimeImmutable $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_timestamp())->as('val'))->toSql(), [typed(
                $input,
                ValueType::TIMESTAMP,
            )]);

        static::assertSame($expected, $result);
    }

    public function test_null_timestamp(): void
    {
        static::assertNull(
            $this
                ->pgsqlContext()
                ->client()
                ->fetchScalar(select(cast(literal(null), column_type_timestamp())->as('val'))->toSql()),
        );
    }

    #[DataProvider('provide_timestamp_values')]
    public function test_timestamp_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_timestamp())->as('val'))->toSql(), [$input]);

        static::assertSame($expected, $result);
    }

    #[DataProvider('provide_timestamp_with_microseconds')]
    public function test_timestamp_with_microseconds(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_timestamp())->as('val'))->toSql(), [$input]);

        if ('' === $expected) {
            static::fail('expected must be non-empty');
        }

        static::assertStringStartsWith($expected, $result);
    }
}
