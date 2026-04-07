<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    col,
    column,
    column_type_integer,
    column_type_serial,
    column_type_varchar,
    create,
    eq,
    gt,
    insert,
    literal,
    merge,
    order_by,
    primary_key,
    select,
    star,
    table
};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class MergeDatabaseTest extends PostgreSqlTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_merge_schema';

    private const SCHEMA_SOURCE = 'flow_postgres_schema_merge_source';

    private const SCHEMA_TARGET = 'flow_postgres_schema_merge_target';

    private const TABLE_SOURCE = 'flow_postgres_merge_source';

    private const TABLE_TARGET = 'flow_postgres_merge_target';

    protected function setUp() : void
    {
        parent::setUp();

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_TARGET)
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_varchar(100))->notNull())
                ->column(column('value', column_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_SOURCE)
                ->column(column('id', column_type_integer())->notNull())
                ->column(column('name', column_type_varchar(100))->notNull())
                ->column(column('value', column_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_TARGET)
                ->columns('name', 'value')
                ->values(literal('Alice'), literal(100))
                ->values(literal('Bob'), literal(200))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_SOURCE)
                ->columns('id', 'name', 'value')
                ->values(literal(1), literal('Alice Updated'), literal(150))
                ->values(literal(2), literal('Bob Updated'), literal(250))
                ->values(literal(3), literal('Charlie'), literal(300))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_SOURCE);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_TARGET);
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME);

        parent::tearDown();
    }

    public function test_merge_delete_matched() : void
    {
        $query = merge(self::TABLE_TARGET, 't')
            ->using(self::TABLE_SOURCE, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatched()
            ->thenDelete();

        self::assertSame(2, $this->pgsqlContext()->client()->execute($query->toSql()));

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TARGET))->toSql()
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

        $this->pgsqlContext()->client()->execute($query->toSql());

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
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

        self::assertSame(1, $this->pgsqlContext()->client()->execute($query->toSql()));

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
        );

        self::assertCount(3, $rows);
        self::assertSame('Charlie', $rows[2]['name']);
        self::assertSame(300, $rows[2]['value']);
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

        self::assertSame(3, $this->pgsqlContext()->client()->execute($query->toSql()));

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
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

        self::assertSame(2, $this->pgsqlContext()->client()->execute($query->toSql()));

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
        );

        self::assertCount(2, $rows);
        self::assertSame('Alice Updated', $rows[0]['name']);
        self::assertSame(150, $rows[0]['value']);
        self::assertSame('Bob Updated', $rows[1]['name']);
        self::assertSame(250, $rows[1]['value']);
    }

    public function test_merge_using_subquery() : void
    {
        $sourceQuery = select(col('id'), col('name'), col('value'))
            ->from(table(self::TABLE_SOURCE))
            ->where(gt(col('value'), literal(200)));

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

        $this->pgsqlContext()->client()->execute($query->toSql());

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
        );

        self::assertCount(3, $rows);
        self::assertSame(250, $rows[1]['value']);
        self::assertSame('Charlie', $rows[2]['name']);
    }

    public function test_merge_with_condition() : void
    {
        $query = merge(self::TABLE_TARGET, 't')
            ->using(self::TABLE_SOURCE, 's')
            ->on(eq(col('t.id'), col('s.id')))
            ->whenMatchedAnd(gt(col('s.value'), literal(200)))
            ->thenUpdate([
                'value' => col('s.value'),
            ]);

        self::assertSame(1, $this->pgsqlContext()->client()->execute($query->toSql()));

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TARGET))->orderBy(order_by(col('id')))->toSql()
        );

        self::assertCount(2, $rows);
        self::assertSame(100, $rows[0]['value']);
        self::assertSame(250, $rows[1]['value']);
    }

    public function test_merge_with_schema_qualified_tables() : void
    {
        $this->pgsqlContext()->client()->execute('CREATE SCHEMA IF NOT EXISTS ' . self::SCHEMA_NAME);

        $this->pgsqlContext()->client()->execute(
            create()->table(self::SCHEMA_TARGET, self::SCHEMA_NAME)
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_varchar(100))->notNull())
                ->column(column('value', column_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table(self::SCHEMA_SOURCE, self::SCHEMA_NAME)
                ->column(column('id', column_type_integer())->notNull())
                ->column(column('name', column_type_varchar(100))->notNull())
                ->column(column('value', column_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::SCHEMA_NAME . '.' . self::SCHEMA_TARGET)
                ->columns('name', 'value')
                ->values(literal('Target Row'), literal(100))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::SCHEMA_NAME . '.' . self::SCHEMA_SOURCE)
                ->columns('id', 'name', 'value')
                ->values(literal(1), literal('Updated Row'), literal(200))
                ->values(literal(2), literal('New Row'), literal(300))
                ->toSql()
        );

        $query = merge(self::SCHEMA_NAME . '.' . self::SCHEMA_TARGET, 't')
            ->using(self::SCHEMA_NAME . '.' . self::SCHEMA_SOURCE, 's')
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

        self::assertSame(2, $this->pgsqlContext()->client()->execute($query->toSql()));

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())
                ->from(table(self::SCHEMA_TARGET, self::SCHEMA_NAME))
                ->orderBy(order_by(col('id')))
                ->toSql()
        );

        self::assertCount(2, $rows);
        self::assertSame('Updated Row', $rows[0]['name']);
        self::assertSame(200, $rows[0]['value']);
        self::assertSame('New Row', $rows[1]['name']);
        self::assertSame(300, $rows[1]['value']);
    }
}
