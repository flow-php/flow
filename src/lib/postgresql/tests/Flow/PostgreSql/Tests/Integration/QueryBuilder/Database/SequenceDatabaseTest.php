<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    alter,
    col,
    create,
    drop,
    eq,
    func,
    literal,
    select,
    table
};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class SequenceDatabaseTest extends PostgreSqlTestCase
{
    private const SEQUENCE_COUNTER = 'flow_postgres_counter_seq';

    private const SEQUENCE_TEST = 'flow_postgres_test_seq';

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropSequenceIfExists(self::SEQUENCE_TEST);
        $this->pgsqlContext()->dropSequenceIfExists(self::SEQUENCE_COUNTER);

        parent::tearDown();
    }

    public function test_alter_sequence_increment() : void
    {
        $createQuery = create()->sequence(self::SEQUENCE_TEST)->startWith(1);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $alterQuery = alter()->sequence(self::SEQUENCE_TEST)
            ->incrementBy(10);

        $this->pgsqlContext()->client()->execute($alterQuery->toSql());

        $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_TEST)]))->toSql()
        );
        $row = $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_TEST)])->as('val'))->toSql()
        );
        self::assertSame(11, $row['val']);
    }

    public function test_alter_sequence_restart() : void
    {
        $createQuery = create()->sequence(self::SEQUENCE_TEST)->startWith(1);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_TEST)]))->toSql()
        );
        $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_TEST)]))->toSql()
        );

        $alterQuery = alter()->sequence(self::SEQUENCE_TEST)
            ->restartWith(1);

        $this->pgsqlContext()->client()->execute($alterQuery->toSql());

        $row = $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_TEST)])->as('val'))->toSql()
        );
        self::assertSame(1, $row['val']);
    }

    public function test_create_sequence() : void
    {
        $query = create()->sequence(self::SEQUENCE_TEST);

        $this->pgsqlContext()->client()->execute($query->toSql());

        $sequences = $this->pgsqlContext()->client()->fetchAll(
            select(col('sequencename'))
                ->from(table('pg_sequences'))
                ->where(eq(col('sequencename'), literal(self::SEQUENCE_TEST)))
                ->toSql()
        );
        self::assertCount(1, $sequences);
    }

    public function test_create_sequence_with_increment() : void
    {
        $query = create()->sequence(self::SEQUENCE_TEST)
            ->incrementBy(5)
            ->startWith(10);

        $this->pgsqlContext()->client()->execute($query->toSql());

        $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_TEST)]))->toSql()
        );
        $row = $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_TEST)])->as('val'))->toSql()
        );
        self::assertSame(15, $row['val']);
    }

    public function test_create_sequence_with_min_max() : void
    {
        $query = create()->sequence(self::SEQUENCE_TEST)
            ->minValue(1)
            ->maxValue(1000)
            ->startWith(1);

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this->pgsqlContext()->client()->fetchSingle(
            select(col('min_value'), col('max_value'))
                ->from(table('pg_sequences'))
                ->where(eq(col('sequencename'), literal(self::SEQUENCE_TEST)))
                ->toSql()
        );
        self::assertSame(1, $row['min_value']);
        self::assertSame(1000, $row['max_value']);
    }

    public function test_create_sequence_with_start_value() : void
    {
        $query = create()->sequence(self::SEQUENCE_TEST)
            ->startWith(100);

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_TEST)])->as('val'))->toSql()
        );
        self::assertSame(100, $row['val']);
    }

    public function test_drop_sequence() : void
    {
        $createQuery = create()->sequence(self::SEQUENCE_TEST);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $dropQuery = drop()->sequence(self::SEQUENCE_TEST);

        $this->pgsqlContext()->client()->execute($dropQuery->toSql());

        $sequences = $this->pgsqlContext()->client()->fetchAll(
            select(col('sequencename'))
                ->from(table('pg_sequences'))
                ->where(eq(col('sequencename'), literal(self::SEQUENCE_TEST)))
                ->toSql()
        );
        self::assertCount(0, $sequences);
    }

    public function test_drop_sequence_if_exists() : void
    {
        $this->expectNotToPerformAssertions();

        $dropQuery = drop()->sequence(self::SEQUENCE_TEST)->ifExists();

        $this->pgsqlContext()->client()->execute($dropQuery->toSql());
    }

    public function test_sequence_usage_with_nextval() : void
    {
        $createQuery = create()->sequence(self::SEQUENCE_COUNTER)->startWith(1);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $val1 = $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_COUNTER)])->as('val'))->toSql()
        );
        $val2 = $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_COUNTER)])->as('val'))->toSql()
        );
        $val3 = $this->pgsqlContext()->client()->fetchSingle(
            select(func('nextval', [literal(self::SEQUENCE_COUNTER)])->as('val'))->toSql()
        );

        self::assertSame(1, $val1['val']);
        self::assertSame(2, $val2['val']);
        self::assertSame(3, $val3['val']);
    }
}
