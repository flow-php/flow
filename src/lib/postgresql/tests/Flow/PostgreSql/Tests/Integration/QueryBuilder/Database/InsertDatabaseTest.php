<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_decimal;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\conflict_columns;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\on_conflict_nothing;
use function Flow\PostgreSql\DSL\on_conflict_update;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\unique_constraint;

final class InsertDatabaseTest extends PostgreSqlTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_insert_schema';

    private const SCHEMA_TABLE = 'flow_postgres_schema_products';

    private const TABLE_PRODUCTS = 'flow_postgres_products';

    protected function setUp(): void
    {
        parent::setUp();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_PRODUCTS)
                    ->column(column('id', column_type_serial()))
                    ->column(column('sku', column_type_varchar(50))->notNull())
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->column(column('price', column_type_decimal(10, 2))->default(0))
                    ->column(column('stock', column_type_integer())->default(0))
                    ->constraint(primary_key('id'))
                    ->constraint(unique_constraint('sku'))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_PRODUCTS);
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME);

        parent::tearDown();
    }

    public function test_basic_insert(): void
    {
        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU001'), literal('Product A'), literal(100));

        static::assertSame(1, $this->pgsqlContext()->client()->execute($query->toSql()));

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('name'))
                    ->from(table(self::TABLE_PRODUCTS))
                    ->where(eq(col('sku'), literal('SKU001')))
                    ->toSql(),
            );
        static::assertSame('Product A', $row['name']);
    }

    public function test_insert_multiple_rows(): void
    {
        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU002'), literal('Product B'), literal(200))
            ->values(literal('SKU003'), literal('Product C'), literal(300));

        static::assertSame(2, $this->pgsqlContext()->client()->execute($query->toSql()));
    }

    public function test_insert_on_conflict_do_nothing(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_PRODUCTS)
                    ->columns('sku', 'name')
                    ->values(literal('SKU006'), literal('Original'))
                    ->toSql(),
            );

        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name')
            ->values(literal('SKU006'), literal('Duplicate'))
            ->onConflict(on_conflict_nothing(conflict_columns(['sku'])));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('name'))
                    ->from(table(self::TABLE_PRODUCTS))
                    ->where(eq(col('sku'), literal('SKU006')))
                    ->toSql(),
            );
        static::assertSame('Original', $row['name']);
    }

    public function test_insert_on_conflict_do_update(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_PRODUCTS)
                    ->columns('sku', 'name', 'price')
                    ->values(literal('SKU007'), literal('Original'), literal(100))
                    ->toSql(),
            );

        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU007'), literal('Updated'), literal(999))
            ->onConflict(on_conflict_update(conflict_columns(['sku']), [
                'name' => literal('Updated'),
                'price' => literal(999),
            ]));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('name'), col('price'))
                    ->from(table(self::TABLE_PRODUCTS))
                    ->where(eq(col('sku'), literal('SKU007')))
                    ->toSql(),
            );
        static::assertSame('Updated', $row['name']);
        static::assertSame('999.00', $row['price']);
    }

    public function test_insert_with_returning(): void
    {
        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU004'), literal('Product D'), literal(400))
            ->returning(col('id'), col('sku'));

        $row = $this->pgsqlContext()->client()->fetchSingle($query->toSql());

        static::assertArrayHasKey('id', $row);
        static::assertArrayHasKey('sku', $row);
        static::assertSame('SKU004', $row['sku']);
    }

    public function test_insert_with_returning_all(): void
    {
        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU005'), literal('Product E'), literal(500))
            ->returningAll();

        $row = $this->pgsqlContext()->client()->fetchSingle($query->toSql());

        static::assertArrayHasKey('id', $row);
        static::assertArrayHasKey('sku', $row);
        static::assertArrayHasKey('name', $row);
        static::assertArrayHasKey('price', $row);
        static::assertArrayHasKey('stock', $row);
    }

    public function test_insert_with_schema_qualified_table(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA_NAME)->ifNotExists()->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::SCHEMA_TABLE, self::SCHEMA_NAME)
                    ->column(column('id', column_type_serial()))
                    ->column(column('sku', column_type_varchar(50))->notNull())
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $query = insert()
            ->into(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)
            ->columns('sku', 'name')
            ->values(literal('SCHEMA-SKU'), literal('Schema Product'));

        static::assertSame(1, $this->pgsqlContext()->client()->execute($query->toSql()));

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('name'))
                    ->from(table(self::SCHEMA_TABLE, self::SCHEMA_NAME))
                    ->where(eq(col('sku'), literal('SCHEMA-SKU')))
                    ->toSql(),
            );
        static::assertSame('Schema Product', $row['name']);
    }
}
