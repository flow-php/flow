<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\agg_count;
use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_decimal;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\gt;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\update;

final class UpdateDatabaseTest extends PostgreSqlTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_update_schema';

    private const SCHEMA_TABLE = 'flow_postgres_schema_employees';

    private const TABLE_DEPARTMENTS = 'flow_postgres_departments';

    private const TABLE_EMPLOYEES = 'flow_postgres_employees';

    protected function setUp(): void
    {
        parent::setUp();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_DEPARTMENTS)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->column(column('bonus_rate', column_type_decimal(3, 2))->default(1))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_EMPLOYEES)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->column(column('salary', column_type_decimal(10, 2))->default(0))
                    ->column(column('department_id', column_type_integer()))
                    ->column(column('status', column_type_varchar(50))->default('active'))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_DEPARTMENTS)
                    ->columns('name', 'bonus_rate')
                    ->values(literal('Engineering'), literal(1))
                    ->values(literal('Sales'), literal(1))
                    ->values(literal('HR'), literal(1))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_EMPLOYEES)
                    ->columns('name', 'salary', 'department_id', 'status')
                    ->values(literal('Alice'), literal(50000), literal(1), literal('active'))
                    ->values(literal('Bob'), literal(60000), literal(1), literal('active'))
                    ->values(literal('Charlie'), literal(55000), literal(2), literal('active'))
                    ->values(literal('Diana'), literal(45000), literal(3), literal('inactive'))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_EMPLOYEES);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_DEPARTMENTS);
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME);

        parent::tearDown();
    }

    public function test_basic_update(): void
    {
        $query = update()->update(self::TABLE_EMPLOYEES)->set('status', literal('updated'));

        static::assertSame(4, $this->pgsqlContext()->client()->execute($query->toSql()));

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(agg_count(star())->as('cnt'))
                    ->from(table(self::TABLE_EMPLOYEES))
                    ->where(eq(col('status'), literal('updated')))
                    ->toSql(),
            );
        static::assertSame(4, $row['cnt']);
    }

    public function test_update_multiple_columns(): void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('salary', literal(75000))
            ->set('status', literal('promoted'))
            ->where(eq(col('name'), literal('Bob')));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('salary'), col('status'))
                    ->from(table(self::TABLE_EMPLOYEES))
                    ->where(eq(col('name'), literal('Bob')))
                    ->toSql(),
            );
        static::assertSame('75000.00', $row['salary']);
        static::assertSame('promoted', $row['status']);
    }

    public function test_update_with_from_clause(): void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('status', literal('bonus_applied'))
            ->from(table(self::TABLE_DEPARTMENTS))
            ->where(and_(
                eq(col('department_id', self::TABLE_EMPLOYEES), col('id', self::TABLE_DEPARTMENTS)),
                gt(col('bonus_rate', self::TABLE_DEPARTMENTS), literal(0)),
            ));

        static::assertGreaterThanOrEqual(1, $this->pgsqlContext()->client()->execute($query->toSql()));
    }

    public function test_update_with_returning(): void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('salary', literal(80000))
            ->where(eq(col('name'), literal('Charlie')))
            ->returning(col('id'), col('name'), col('salary'));

        $row = $this->pgsqlContext()->client()->fetchSingle($query->toSql());

        static::assertSame('Charlie', $row['name']);
        static::assertSame('80000.00', $row['salary']);
    }

    public function test_update_with_returning_all(): void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('status', literal('reviewed'))
            ->where(eq(col('name'), literal('Diana')))
            ->returningAll();

        $row = $this->pgsqlContext()->client()->fetchSingle($query->toSql());

        static::assertArrayHasKey('id', $row);
        static::assertArrayHasKey('name', $row);
        static::assertArrayHasKey('salary', $row);
        static::assertArrayHasKey('department_id', $row);
        static::assertArrayHasKey('status', $row);
        static::assertSame('reviewed', $row['status']);
    }

    public function test_update_with_schema_qualified_table(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->ifNotExists()->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::SCHEMA_TABLE, self::SCHEMA_NAME)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->column(column('status', column_type_varchar(50))->default('active'))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)
                    ->columns('name', 'status')
                    ->values(literal('Alice'), literal('active'))
                    ->toSql(),
            );

        $query = update()
            ->update(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)
            ->set('status', literal('updated'))
            ->where(eq(col('name'), literal('Alice')));

        static::assertSame(1, $this->pgsqlContext()->client()->execute($query->toSql()));

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('status'))
                    ->from(table(self::SCHEMA_TABLE, self::SCHEMA_NAME))
                    ->where(eq(col('name'), literal('Alice')))
                    ->toSql(),
            );
        static::assertSame('updated', $row['status']);
    }

    public function test_update_with_where(): void
    {
        $query = update()
            ->update(self::TABLE_EMPLOYEES)
            ->set('salary', literal(70000))
            ->where(eq(col('name'), literal('Alice')));

        static::assertSame(1, $this->pgsqlContext()->client()->execute($query->toSql()));

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('salary'))
                    ->from(table(self::TABLE_EMPLOYEES))
                    ->where(eq(col('name'), literal('Alice')))
                    ->toSql(),
            );
        static::assertSame('70000.00', $row['salary']);
    }
}
