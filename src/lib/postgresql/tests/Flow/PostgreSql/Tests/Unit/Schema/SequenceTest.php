<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_sequence;

final class SequenceTest extends TestCase
{
    public function test_sequence_construction(): void
    {
        $seq = schema_sequence('users_id_seq');

        static::assertSame('users_id_seq', $seq->name);
        static::assertSame('bigint', $seq->dataType);
        static::assertSame(1, $seq->startValue);
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
            'CREATE SEQUENCE users_id_seq AS bigint START 1 INCREMENT 1 MINVALUE 1 CACHE 1 NO MAXVALUE',
            schema_sequence('users_id_seq')->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_create_sequence_with_all_options(): void
    {
        static::assertSame(
            'CREATE SEQUENCE custom_seq AS int START 100 INCREMENT 5 MINVALUE 1 CACHE 10 MAXVALUE 10000 CYCLE OWNED BY users.id',
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
