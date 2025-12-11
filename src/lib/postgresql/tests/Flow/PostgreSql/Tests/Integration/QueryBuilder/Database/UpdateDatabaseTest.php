<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    agg_count,
    col,
    column,
    cond_and,
    create,
    eq,
    gt,
    insert,
    literal,
    primary_key,
    select,
    sql_type_decimal,
    sql_type_integer,
    sql_type_serial,
    sql_type_varchar,
    star,
    table,
    update
};

final class UpdateDatabaseTest extends DatabaseTestCase
{
    private const TABLE_DEPARTMENTS = 'flow_postgres_departments';

    private const TABLE_EMPLOYEES = 'flow_postgres_employees';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_DEPARTMENTS)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->column(column('bonus_rate', sql_type_decimal(3, 2))->default(1))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_EMPLOYEES)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->column(column('salary', sql_type_decimal(10, 2))->default(0))
                ->column(column('department_id', sql_type_integer()))
                ->column(column('status', sql_type_varchar(50))->default('active'))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_DEPARTMENTS)
                ->columns('name', 'bonus_rate')
                ->values(literal('Engineering'), literal(1))
                ->values(literal('Sales'), literal(1))
                ->values(literal('HR'), literal(1))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_EMPLOYEES)
                ->columns('name', 'salary', 'department_id', 'status')
                ->values(literal('Alice'), literal(50000), literal(1), literal('active'))
                ->values(literal('Bob'), literal(60000), literal(1), literal('active'))
                ->values(literal('Charlie'), literal(55000), literal(2), literal('active'))
                ->values(literal('Diana'), literal(45000), literal(3), literal('inactive'))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_EMPLOYEES);
        $this->dropTableIfExists(self::TABLE_DEPARTMENTS);

        parent::tearDown();
    }

    public function test_basic_update() : void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('status', literal('updated'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(4, $this->affectedRows($result));

        $check = $this->execute(
            select(agg_count(star())->as('cnt'))
                ->from(table(self::TABLE_EMPLOYEES))
                ->where(eq(col('status'), literal('updated')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('4', $row['cnt']);
    }

    public function test_update_multiple_columns() : void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('salary', literal(75000))
            ->set('status', literal('promoted'))
            ->where(eq(col('name'), literal('Bob')));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute(
            select(col('salary'), col('status'))
                ->from(table(self::TABLE_EMPLOYEES))
                ->where(eq(col('name'), literal('Bob')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('75000.00', $row['salary']);
        self::assertSame('promoted', $row['status']);
    }

    public function test_update_with_from_clause() : void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('status', literal('bonus_applied'))
            ->from(table(self::TABLE_DEPARTMENTS))
            ->where(
                cond_and(
                    eq(
                        col('department_id', self::TABLE_EMPLOYEES),
                        col('id', self::TABLE_DEPARTMENTS)
                    ),
                    gt(col('bonus_rate', self::TABLE_DEPARTMENTS), literal(0))
                )
            );

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertGreaterThanOrEqual(1, $this->affectedRows($result));
    }

    public function test_update_with_returning() : void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('salary', literal(80000))
            ->where(eq(col('name'), literal('Charlie')))
            ->returning(col('id'), col('name'), col('salary'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $row = $this->fetchOne($result);
        self::assertSame('Charlie', $row['name']);
        self::assertSame('80000.00', $row['salary']);
    }

    public function test_update_with_returning_all() : void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('status', literal('reviewed'))
            ->where(eq(col('name'), literal('Diana')))
            ->returningAll();

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $row = $this->fetchOne($result);
        self::assertArrayHasKey('id', $row);
        self::assertArrayHasKey('name', $row);
        self::assertArrayHasKey('salary', $row);
        self::assertArrayHasKey('department_id', $row);
        self::assertArrayHasKey('status', $row);
        self::assertSame('reviewed', $row['status']);
    }

    public function test_update_with_where() : void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('salary', literal(70000))
            ->where(eq(col('name'), literal('Alice')));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(1, $this->affectedRows($result));

        $check = $this->execute(
            select(col('salary'))
                ->from(table(self::TABLE_EMPLOYEES))
                ->where(eq(col('name'), literal('Alice')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('70000.00', $row['salary']);
    }
}
