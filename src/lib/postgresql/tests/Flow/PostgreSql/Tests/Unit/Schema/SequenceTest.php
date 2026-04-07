<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\schema_sequence;

use PHPUnit\Framework\TestCase;

final class SequenceTest extends TestCase
{
    public function test_sequence_construction() : void
    {
        $seq = schema_sequence('users_id_seq');

        self::assertSame('users_id_seq', $seq->name);
        self::assertSame('bigint', $seq->dataType);
        self::assertSame(1, $seq->startValue);
    }

    public function test_sequence_owned_by() : void
    {
        $seq = schema_sequence('users_id_seq', ownedByTable: 'users', ownedByColumn: 'id');

        self::assertSame('users', $seq->ownedByTable);
        self::assertSame('id', $seq->ownedByColumn);
    }

    public function test_sequence_with_all_options() : void
    {
        $seq = schema_sequence('custom_seq', dataType: 'integer', startValue: 100, minValue: 1, maxValue: 10000, incrementBy: 5, cycle: true, cacheValue: 10);

        self::assertSame('integer', $seq->dataType);
        self::assertSame(100, $seq->startValue);
        self::assertSame(10000, $seq->maxValue);
        self::assertSame(5, $seq->incrementBy);
        self::assertTrue($seq->cycle);
        self::assertSame(10, $seq->cacheValue);
    }

    public function test_to_sql_generates_create_sequence() : void
    {
        self::assertSame(
            'CREATE SEQUENCE users_id_seq AS bigint START 1 INCREMENT 1 MINVALUE 1 CACHE 1 NO MAXVALUE',
            schema_sequence('users_id_seq')->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_create_sequence_with_all_options() : void
    {
        self::assertSame(
            'CREATE SEQUENCE custom_seq AS int START 100 INCREMENT 5 MINVALUE 1 CACHE 10 MAXVALUE 10000 CYCLE OWNED BY users.id',
            schema_sequence('custom_seq', dataType: 'integer', startValue: 100, minValue: 1, maxValue: 10000, incrementBy: 5, cycle: true, cacheValue: 10, ownedByTable: 'users', ownedByColumn: 'id')->toSql()->toSql(),
        );
    }
}
