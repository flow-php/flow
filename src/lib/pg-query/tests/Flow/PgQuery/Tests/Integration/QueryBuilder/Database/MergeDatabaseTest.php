<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Database;

use function Flow\PgQuery\DSL\{
    col,
    column,
    create_table,
    eq,
    gt,
    insert,
    literal_int,
    literal_string,
    merge,
    order_by,
    primary_key,
    select,
    sql_type_integer,
    sql_type_serial,
    sql_type_varchar,
    star,
    table
};

final class MergeDatabaseTest extends DatabaseTestCase
{
    private const TABLE_SOURCE = 'flow_postgres_merge_source';

    private const TABLE_TARGET = 'flow_postgres_merge_target';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create_table(self::TABLE_TARGET)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->column(column('value', sql_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            create_table(self::TABLE_SOURCE)
                ->column(column('id', sql_type_integer())->notNull())
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->column(column('value', sql_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_TARGET)
                ->columns('name', 'value')
                ->values(literal_string('Alice'), literal_int(100))
                ->values(literal_string('Bob'), literal_int(200))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_SOURCE)
                ->columns('id', 'name', 'value')
                ->values(literal_int(1), literal_string('Alice Updated'), literal_int(150))
                ->values(literal_int(2), literal_string('Bob Updated'), literal_int(250))
                ->values(literal_int(3), literal_string('Charlie'), literal_int(300))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_SOURCE);
        $this->dropTableIfExists(self::TABLE_TARGET);

        parent::tearDown();
    }

    public function test_merge_delete_matched() : void
    {
        $query = merge(self::TABLE_TARGET, 't')
            ->using(self::TABLE_SOURCE, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatched()
            ->thenDelete();

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(2, $this->affectedRows($result));

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TARGET))->toSql()
            )
        );

        self::assertCount(0, $rows);
    }

    public function test_merge_do_nothing() : void
    {
        $query = merge(self::TABLE_TARGET, 't')
            ->using(self::TABLE_SOURCE, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatched()
            ->thenDoNothing();

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
            )
        );

        self::assertCount(2, $rows);
        self::assertSame('Alice', $rows[0]['name']);
        self::assertSame('Bob', $rows[1]['name']);
    }

    public function test_merge_insert_not_matched() : void
    {
        $query = merge(self::TABLE_TARGET, 't')
            ->using(self::TABLE_SOURCE, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenNotMatched()
            ->thenInsertValues([
                'name' => col('s.name'),
                'value' => col('s.value'),
            ]);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(1, $this->affectedRows($result));

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
            )
        );

        self::assertCount(3, $rows);
        self::assertSame('Charlie', $rows[2]['name']);
        self::assertSame('300', $rows[2]['value']);
    }

    public function test_merge_update_and_insert() : void
    {
        $query = merge(self::TABLE_TARGET, 't')
            ->using(self::TABLE_SOURCE, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatched()
            ->thenUpdate([
                'name' => col('s.name'),
                'value' => col('s.value'),
            ])
            ->whenNotMatched()
            ->thenInsertValues([
                'name' => col('s.name'),
                'value' => col('s.value'),
            ]);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(3, $this->affectedRows($result));

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
            )
        );

        self::assertCount(3, $rows);
        self::assertSame('Alice Updated', $rows[0]['name']);
        self::assertSame('Bob Updated', $rows[1]['name']);
        self::assertSame('Charlie', $rows[2]['name']);
    }

    public function test_merge_update_matched() : void
    {
        $query = merge(self::TABLE_TARGET, 't')
            ->using(self::TABLE_SOURCE, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatched()
            ->thenUpdate([
                'name' => col('s.name'),
                'value' => col('s.value'),
            ]);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(2, $this->affectedRows($result));

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
            )
        );

        self::assertCount(2, $rows);
        self::assertSame('Alice Updated', $rows[0]['name']);
        self::assertSame('150', $rows[0]['value']);
        self::assertSame('Bob Updated', $rows[1]['name']);
        self::assertSame('250', $rows[1]['value']);
    }

    public function test_merge_using_subquery() : void
    {
        $sourceQuery = select(col('id'), col('name'), col('value'))
            ->from(table(self::TABLE_SOURCE))
            ->where(gt(col('value'), literal_int(200)));

        $query = merge(self::TABLE_TARGET, 't')
            ->using($sourceQuery, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatched()
            ->thenUpdate([
                'value' => col('s.value'),
            ])
            ->whenNotMatched()
            ->thenInsertValues([
                'name' => col('s.name'),
                'value' => col('s.value'),
            ]);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
            )
        );

        self::assertCount(3, $rows);
        self::assertSame('250', $rows[1]['value']);
        self::assertSame('Charlie', $rows[2]['name']);
    }

    public function test_merge_with_condition() : void
    {
        $query = merge(self::TABLE_TARGET, 't')
            ->using(self::TABLE_SOURCE, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatchedAnd(gt(col('s.value'), literal_int(200)))
            ->thenUpdate([
                'value' => col('s.value'),
            ]);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(1, $this->affectedRows($result));

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
            )
        );

        self::assertCount(2, $rows);
        self::assertSame('100', $rows[0]['value']);
        self::assertSame('250', $rows[1]['value']);
    }
}
