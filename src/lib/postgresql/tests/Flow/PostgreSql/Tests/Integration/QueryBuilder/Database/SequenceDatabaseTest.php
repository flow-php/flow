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

final class SequenceDatabaseTest extends DatabaseTestCase
{
    private const SEQUENCE_COUNTER = 'flow_postgres_counter_seq';

    private const SEQUENCE_TEST = 'flow_postgres_test_seq';

    protected function tearDown() : void
    {
        $this->dropSequenceIfExists(self::SEQUENCE_TEST);
        $this->dropSequenceIfExists(self::SEQUENCE_COUNTER);

        parent::tearDown();
    }

    public function test_alter_sequence_increment() : void
    {
        $createQuery = create()->sequence(self::SEQUENCE_TEST)->startWith(1);
        $this->execute($createQuery->toSql());

        $alterQuery = alter()->sequence(self::SEQUENCE_TEST)
            ->incrementBy(10);

        $result = $this->execute($alterQuery->toSql());

        self::assertNotFalse($result);

        $this->execute(
            select(func('nextval', [literal(self::SEQUENCE_TEST)]))->toSql()
        );
        $secondVal = $this->execute(
            select(func('nextval', [literal(self::SEQUENCE_TEST)])->as('val'))->toSql()
        );
        $row = $this->fetchOne($secondVal);
        self::assertSame('11', $row['val']);
    }

    public function test_alter_sequence_restart() : void
    {
        $createQuery = create()->sequence(self::SEQUENCE_TEST)->startWith(1);
        $this->execute($createQuery->toSql());

        $this->execute(
            select(func('nextval', [literal(self::SEQUENCE_TEST)]))->toSql()
        );
        $this->execute(
            select(func('nextval', [literal(self::SEQUENCE_TEST)]))->toSql()
        );

        $alterQuery = alter()->sequence(self::SEQUENCE_TEST)
            ->restartWith(1);

        $result = $this->execute($alterQuery->toSql());

        self::assertNotFalse($result);

        $nextVal = $this->execute(
            select(func('nextval', [literal(self::SEQUENCE_TEST)])->as('val'))->toSql()
        );
        $row = $this->fetchOne($nextVal);
        self::assertSame('1', $row['val']);
    }

    public function test_create_sequence() : void
    {
        $query = create()->sequence(self::SEQUENCE_TEST);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute(
            select(col('sequencename'))
                ->from(table('pg_sequences'))
                ->where(eq(col('sequencename'), literal(self::SEQUENCE_TEST)))
                ->toSql()
        );
        $sequences = $this->fetchAll($check);
        self::assertCount(1, $sequences);
    }

    public function test_create_sequence_with_increment() : void
    {
        $query = create()->sequence(self::SEQUENCE_TEST)
            ->incrementBy(5)
            ->startWith(10);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $this->execute(
            select(func('nextval', [literal(self::SEQUENCE_TEST)]))->toSql()
        );
        $secondVal = $this->execute(
            select(func('nextval', [literal(self::SEQUENCE_TEST)])->as('val'))->toSql()
        );
        $row = $this->fetchOne($secondVal);
        self::assertSame('15', $row['val']);
    }

    public function test_create_sequence_with_min_max() : void
    {
        $query = create()->sequence(self::SEQUENCE_TEST)
            ->minValue(1)
            ->maxValue(1000)
            ->startWith(1);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute(
            select(col('min_value'), col('max_value'))
                ->from(table('pg_sequences'))
                ->where(eq(col('sequencename'), literal(self::SEQUENCE_TEST)))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('1', $row['min_value']);
        self::assertSame('1000', $row['max_value']);
    }

    public function test_create_sequence_with_start_value() : void
    {
        $query = create()->sequence(self::SEQUENCE_TEST)
            ->startWith(100);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $nextVal = $this->execute(
            select(func('nextval', [literal(self::SEQUENCE_TEST)])->as('val'))->toSql()
        );
        $row = $this->fetchOne($nextVal);
        self::assertSame('100', $row['val']);
    }

    public function test_drop_sequence() : void
    {
        $createQuery = create()->sequence(self::SEQUENCE_TEST);
        $this->execute($createQuery->toSql());

        $dropQuery = drop()->sequence(self::SEQUENCE_TEST);

        $result = $this->execute($dropQuery->toSql());

        self::assertNotFalse($result);

        $check = $this->execute(
            select(col('sequencename'))
                ->from(table('pg_sequences'))
                ->where(eq(col('sequencename'), literal(self::SEQUENCE_TEST)))
                ->toSql()
        );
        $sequences = $this->fetchAll($check);
        self::assertCount(0, $sequences);
    }

    public function test_drop_sequence_if_exists() : void
    {
        $dropQuery = drop()->sequence(self::SEQUENCE_TEST)->ifExists();

        $result = $this->execute($dropQuery->toSql());

        self::assertNotFalse($result);
    }

    public function test_sequence_usage_with_nextval() : void
    {
        $createQuery = create()->sequence(self::SEQUENCE_COUNTER)->startWith(1);
        $this->execute($createQuery->toSql());

        $val1 = $this->fetchOne($this->execute(
            select(func('nextval', [literal(self::SEQUENCE_COUNTER)])->as('val'))->toSql()
        ));
        $val2 = $this->fetchOne($this->execute(
            select(func('nextval', [literal(self::SEQUENCE_COUNTER)])->as('val'))->toSql()
        ));
        $val3 = $this->fetchOne($this->execute(
            select(func('nextval', [literal(self::SEQUENCE_COUNTER)])->as('val'))->toSql()
        ));

        self::assertSame('1', $val1['val']);
        self::assertSame('2', $val2['val']);
        self::assertSame('3', $val3['val']);
    }
}
