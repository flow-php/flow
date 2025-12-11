<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    col,
    column,
    conflict_columns,
    create,
    eq,
    insert,
    literal,
    on_conflict_nothing,
    on_conflict_update,
    primary_key,
    select,
    sql_type_decimal,
    sql_type_integer,
    sql_type_serial,
    sql_type_varchar,
    table,
    unique_constraint
};

final class InsertDatabaseTest extends DatabaseTestCase
{
    private const TABLE_PRODUCTS = 'flow_postgres_products';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_PRODUCTS)
                ->column(column('id', sql_type_serial()))
                ->column(column('sku', sql_type_varchar(50))->notNull())
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->column(column('price', sql_type_decimal(10, 2))->default(0))
                ->column(column('stock', sql_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->constraint(unique_constraint('sku'))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_PRODUCTS);

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
}
