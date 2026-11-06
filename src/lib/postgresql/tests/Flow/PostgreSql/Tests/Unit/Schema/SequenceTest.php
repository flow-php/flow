<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\Sequence;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_sequence;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class SequenceTest extends TestCase
{
    public function test_schema_round_trips_through_normalize(): void
    {
        $sequence = schema_sequence('s', 'integer', 5, 2, 100, 3, true, 10, 'orders', 'id', 'pivotly');

        static::assertEquals($sequence, Sequence::fromArray($sequence->normalize()));
    }

    public function test_with_schema_qualifies_name_and_keeps_every_other_field(): void
    {
        $sequence = schema_sequence('s', 'integer', 5, 2, 100, 3, true, 10, 'orders', 'id');

        static::assertSame('public.s', $sequence->qualifiedName());
        static::assertEquals(
            schema_sequence('s', 'integer', 5, 2, 100, 3, true, 10, 'orders', 'id', 'pivotly'),
            $sequence->withSchema('pivotly'),
        );
        static::assertSame('pivotly.s', $sequence->withSchema('pivotly')->qualifiedName());
    }

    /**
     * @return Generator<string, array{string, int, int}>
     */
    public static function default_max_values(): Generator
    {
        yield 'smallint ascending' => ['smallint', 1, 32_767];
        yield 'integer ascending' => ['integer', 1, 2_147_483_647];
        yield 'bigint ascending' => ['bigint', 1, PHP_INT_MAX];
        yield 'smallint descending' => ['smallint', -1, -1];
        yield 'integer descending' => ['integer', -1, -1];
        yield 'bigint descending' => ['bigint', -1, -1];
    }

    #[DataProvider('default_max_values')]
    public function test_effective_max_value_defaults_per_data_type_and_direction(
        string $dataType,
        int $incrementBy,
        int $expected,
    ): void {
        static::assertSame($expected, schema_sequence('s', $dataType, incrementBy: $incrementBy)->effectiveMaxValue());
    }

    /**
     * @return Generator<string, array{string, int}>
     */
    public static function data_types_and_directions(): Generator
    {
        foreach (['smallint', 'integer', 'bigint'] as $dataType) {
            yield $dataType . ' ascending' => [$dataType, 1];
            yield $dataType . ' descending' => [$dataType, -1];
        }
    }

    /**
     * @return Generator<string, array{string, int, int, int}>
     */
    public static function default_start_and_min_values(): Generator
    {
        yield 'smallint ascending' => ['smallint', 1, 1, 1];
        yield 'integer ascending' => ['integer', 1, 1, 1];
        yield 'bigint ascending' => ['bigint', 1, 1, 1];
        yield 'smallint descending' => ['smallint', -1, -1, -32_768];
        yield 'integer descending' => ['integer', -1, -1, -2_147_483_648];
        yield 'bigint descending' => ['bigint', -1, -1, PHP_INT_MIN];
    }

    #[DataProvider('default_start_and_min_values')]
    public function test_effective_start_and_min_values_default_per_data_type_and_direction(
        string $dataType,
        int $incrementBy,
        int $expectedStart,
        int $expectedMin,
    ): void {
        $sequence = schema_sequence('s', $dataType, incrementBy: $incrementBy);

        static::assertSame($expectedStart, $sequence->effectiveStartValue());
        static::assertSame($expectedMin, $sequence->effectiveMinValue());
    }

    #[DataProvider('data_types_and_directions')]
    public function test_effective_start_and_min_values_prefer_explicit_values(string $dataType, int $incrementBy): void
    {
        $sequence = schema_sequence('s', $dataType, startValue: 7, minValue: -7, incrementBy: $incrementBy);

        static::assertSame(7, $sequence->effectiveStartValue());
        static::assertSame(-7, $sequence->effectiveMinValue());
    }

    public function test_default_sequence_omits_start_and_min_value(): void
    {
        static::assertSame(
            'CREATE SEQUENCE public.x AS bigint INCREMENT 1 NO MINVALUE CACHE 1 NO MAXVALUE',
            schema_sequence('x')->toSql()->toSql(),
        );
    }

    public function test_descending_default_sequence_omits_start_and_min_value(): void
    {
        static::assertSame(
            'CREATE SEQUENCE public.x AS bigint INCREMENT -1 NO MINVALUE CACHE 1 NO MAXVALUE',
            schema_sequence('x', incrementBy: -1)->toSql()->toSql(),
        );
    }

    #[DataProvider('data_types_and_directions')]
    public function test_effective_max_value_prefers_explicit_value(string $dataType, int $incrementBy): void
    {
        static::assertSame(
            100,
            schema_sequence('s', $dataType, maxValue: 100, incrementBy: $incrementBy)->effectiveMaxValue(),
        );
    }

    public function test_sequence_construction(): void
    {
        $seq = schema_sequence('users_id_seq');

        static::assertSame('users_id_seq', $seq->name);
        static::assertSame('bigint', $seq->dataType);
        static::assertNull($seq->startValue);
        static::assertSame(1, $seq->effectiveStartValue());
    }

    public function test_sequence_owned_by(): void
    {
        $seq = schema_sequence('users_id_seq', ownedByTable: 'users', ownedByColumn: 'id');

        static::assertSame('users', $seq->ownedByTable);
        static::assertSame('id', $seq->ownedByColumn);
    }

    public function test_sequence_with_all_options(): void
    {
        $seq = schema_sequence(
            'custom_seq',
            dataType: 'integer',
            startValue: 100,
            minValue: 1,
            maxValue: 10000,
            incrementBy: 5,
            cycle: true,
            cacheValue: 10,
        );

        static::assertSame('integer', $seq->dataType);
        static::assertSame(100, $seq->startValue);
        static::assertSame(10000, $seq->maxValue);
        static::assertSame(5, $seq->incrementBy);
        static::assertTrue($seq->cycle);
        static::assertSame(10, $seq->cacheValue);
    }

    public function test_to_sql_generates_create_sequence(): void
    {
        static::assertSame(
            'CREATE SEQUENCE public.users_id_seq AS bigint INCREMENT 1 NO MINVALUE CACHE 1 NO MAXVALUE',
            schema_sequence('users_id_seq')->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_create_sequence_with_all_options(): void
    {
        static::assertSame(
            'CREATE SEQUENCE public.custom_seq AS int START 100 INCREMENT 5 MINVALUE 1 CACHE 10 MAXVALUE 10000 CYCLE OWNED BY public.users.id',
            schema_sequence(
                'custom_seq',
                dataType: 'integer',
                startValue: 100,
                minValue: 1,
                maxValue: 10000,
                incrementBy: 5,
                cycle: true,
                cacheValue: 10,
                ownedByTable: 'users',
                ownedByColumn: 'id',
            )
                ->toSql()
                ->toSql(),
        );
    }
}
