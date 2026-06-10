<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_timestamptz;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;

final class TimestampTzConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_timestamptz_values(): Generator
    {
        yield 'utc timestamp' => ['2024-03-15 14:30:00+00'];
        yield 'positive offset' => ['2024-03-15 16:30:00+02'];
        yield 'negative offset' => ['2024-03-15 09:30:00-05'];
    }

    public function test_null_timestamptz(): void
    {
        static::assertNull(
            $this
                ->pgsqlContext()
                ->client()
                ->fetchScalar(select(cast(literal(null), column_type_timestamptz())->as('val'))->toSql()),
        );
    }

    #[DataProvider('provide_timestamptz_values')]
    public function test_timestamptz_round_trip(string $input): void
    {
        static::assertIsString(
            $this
                ->pgsqlContext()
                ->client()
                ->fetchScalarString(select(cast(param(1), column_type_timestamptz())->as('val'))->toSql(), [$input]),
        );
    }
}
