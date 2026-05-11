<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\SequenceDiff;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_sequence;

final class SequenceDiffTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_generates_alter_cache_value(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq', cacheValue: 1),
            schema_sequence('users_id_seq', cacheValue: 20),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq CACHE 20', $sqls[0]->toSql());
    }

    public function test_generates_alter_cycle_to_no_cycle(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq', cycle: true),
            schema_sequence('users_id_seq', cycle: false),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq NO CYCLE', $sqls[0]->toSql());
    }

    public function test_generates_alter_data_type(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq', dataType: 'bigint'),
            schema_sequence('users_id_seq', dataType: 'integer'),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq AS int', $sqls[0]->toSql());
    }

    public function test_generates_alter_increment_by(): void
    {
        $diff = new SequenceDiff(schema_sequence('users_id_seq'), schema_sequence('users_id_seq', incrementBy: 10));

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq INCREMENT 10', $sqls[0]->toSql());
    }

    public function test_generates_alter_min_value(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq', minValue: 1),
            schema_sequence('users_id_seq', minValue: 50),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq MINVALUE 50', $sqls[0]->toSql());
    }

    public function test_generates_alter_multiple_properties(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq'),
            schema_sequence('users_id_seq', incrementBy: 10, cycle: true, maxValue: 1000),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq INCREMENT 10 MAXVALUE 1000 CYCLE', $sqls[0]->toSql());
    }

    public function test_generates_alter_no_max_value(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq', maxValue: 1000),
            schema_sequence('users_id_seq', maxValue: null),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq NO MAXVALUE', $sqls[0]->toSql());
    }

    public function test_generates_alter_owned_by(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq'),
            schema_sequence('users_id_seq', ownedByTable: 'users', ownedByColumn: 'id'),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq OWNED BY users.id', $sqls[0]->toSql());
    }

    public function test_generates_alter_owned_by_none(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq', ownedByTable: 'users', ownedByColumn: 'id'),
            schema_sequence('users_id_seq'),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq OWNED BY "none"', $sqls[0]->toSql());
    }

    public function test_generates_alter_start_value(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq', startValue: 1),
            schema_sequence('users_id_seq', startValue: 100),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq START 100', $sqls[0]->toSql());
    }

    public function test_returns_empty_when_no_changes(): void
    {
        $diff = new SequenceDiff(schema_sequence('users_id_seq'), schema_sequence('users_id_seq'));

        static::assertSame([], $diff->generate());
    }

    public function test_reversed_increment_change(): void
    {
        $diff = new SequenceDiff(
            schema_sequence('users_id_seq', incrementBy: 10),
            schema_sequence('users_id_seq', incrementBy: 1),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER SEQUENCE users_id_seq INCREMENT 1', $sqls[0]->toSql());
    }
}
