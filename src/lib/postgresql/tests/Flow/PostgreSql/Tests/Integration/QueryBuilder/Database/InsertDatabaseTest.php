<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    col,
    column,
    conflict_columns,
    create,
    data_type_decimal,
    data_type_integer,
    data_type_serial,
    data_type_varchar,
    eq,
    insert,
    literal,
    on_conflict_nothing,
    on_conflict_update,
    primary_key,
    select,
    table,
    unique_constraint
};

final class InsertDatabaseTest extends DatabaseTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_insert_schema';

    private const SCHEMA_TABLE = 'flow_postgres_schema_products';

    private const TABLE_PRODUCTS = 'flow_postgres_products';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_PRODUCTS)
                ->column(column('id', data_type_serial()))
                ->column(column('sku', data_type_varchar(50))->notNull())
                ->column(column('name', data_type_varchar(100))->notNull())
                ->column(column('price', data_type_decimal(10, 2))->default(0))
                ->column(column('stock', data_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->constraint(unique_constraint('sku'))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_PRODUCTS);
        $this->execute('DROP SCHEMA IF EXISTS ' . self::SCHEMA_NAME . ' CASCADE');

        parent::tearDown();
    }

    public function test_basic_insert() : void
    {
        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU001'), literal('Product A'), literal(100));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(1, $this->affectedRows($result));

        $check = $this->execute(
            select(col('name'))
                ->from(table(self::TABLE_PRODUCTS))
                ->where(eq(col('sku'), literal('SKU001')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('Product A', $row['name']);
    }

    public function test_insert_multiple_rows() : void
    {
        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU002'), literal('Product B'), literal(200))
            ->values(literal('SKU003'), literal('Product C'), literal(300));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(2, $this->affectedRows($result));
    }

    public function test_insert_on_conflict_do_nothing() : void
    {
        $this->execute(
            insert()
                ->into(self::TABLE_PRODUCTS)
                ->columns('sku', 'name')
                ->values(literal('SKU006'), literal('Original'))
                ->toSql()
        );

        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name')
            ->values(literal('SKU006'), literal('Duplicate'))
            ->onConflict(on_conflict_nothing(conflict_columns(['sku'])));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute(
            select(col('name'))
                ->from(table(self::TABLE_PRODUCTS))
                ->where(eq(col('sku'), literal('SKU006')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('Original', $row['name']);
    }

    public function test_insert_on_conflict_do_update() : void
    {
        $this->execute(
            insert()
                ->into(self::TABLE_PRODUCTS)
                ->columns('sku', 'name', 'price')
                ->values(literal('SKU007'), literal('Original'), literal(100))
                ->toSql()
        );

        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU007'), literal('Updated'), literal(999))
            ->onConflict(
                on_conflict_update(
                    conflict_columns(['sku']),
                    ['name' => literal('Updated'), 'price' => literal(999)]
                )
            );

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute(
            select(col('name'), col('price'))
                ->from(table(self::TABLE_PRODUCTS))
                ->where(eq(col('sku'), literal('SKU007')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('Updated', $row['name']);
        self::assertSame('999.00', $row['price']);
    }

    public function test_insert_with_returning() : void
    {
        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU004'), literal('Product D'), literal(400))
            ->returning(col('id'), col('sku'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $row = $this->fetchOne($result);
        self::assertArrayHasKey('id', $row);
        self::assertArrayHasKey('sku', $row);
        self::assertSame('SKU004', $row['sku']);
    }

    public function test_insert_with_returning_all() : void
    {
        $query = insert()
            ->into(self::TABLE_PRODUCTS)
            ->columns('sku', 'name', 'price')
            ->values(literal('SKU005'), literal('Product E'), literal(500))
            ->returningAll();

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $row = $this->fetchOne($result);
        self::assertArrayHasKey('id', $row);
        self::assertArrayHasKey('sku', $row);
        self::assertArrayHasKey('name', $row);
        self::assertArrayHasKey('price', $row);
        self::assertArrayHasKey('stock', $row);
    }

    public function test_insert_with_schema_qualified_table() : void
    {
        $this->execute('CREATE SCHEMA IF NOT EXISTS ' . self::SCHEMA_NAME);

        $this->execute(
            create()->table(self::SCHEMA_TABLE, self::SCHEMA_NAME)
                ->column(column('id', data_type_serial()))
                ->column(column('sku', data_type_varchar(50))->notNull())
                ->column(column('name', data_type_varchar(100))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $query = insert()
            ->into(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)
            ->columns('sku', 'name')
            ->values(literal('SCHEMA-SKU'), literal('Schema Product'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(1, $this->affectedRows($result));

        $check = $this->execute(
            select(col('name'))
                ->from(table(self::SCHEMA_TABLE, self::SCHEMA_NAME))
                ->where(eq(col('sku'), literal('SCHEMA-SKU')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('Schema Product', $row['name']);
    }
}
