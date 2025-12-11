<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    agg_sum,
    col,
    column,
    cond_and,
    create,
    cte,
    desc,
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
    with
};

final class SelectDatabaseTest extends DatabaseTestCase
{
    private const TABLE_ORDERS = 'flow_postgres_orders';

    private const TABLE_USERS = 'flow_postgres_users';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_USERS)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->column(column('email', sql_type_varchar(255)))
                ->column(column('age', sql_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_ORDERS)
                ->column(column('id', sql_type_serial()))
                ->column(column('user_id', sql_type_integer()))
                ->column(column('amount', sql_type_decimal(10, 2))->notNull())
                ->column(column('status', sql_type_varchar(50))->default('pending'))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_USERS)
                ->columns('name', 'email', 'age')
                ->values(literal('John Doe'), literal('john@example.com'), literal(30))
                ->values(literal('Jane Smith'), literal('jane@example.com'), literal(25))
                ->values(literal('Bob Wilson'), literal('bob@example.com'), literal(35))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_ORDERS)
                ->columns('user_id', 'amount', 'status')
                ->values(literal(1), literal(100), literal('completed'))
                ->values(literal(1), literal(200), literal('pending'))
                ->values(literal(2), literal(150), literal('completed'))
                ->values(literal(3), literal(300), literal('completed'))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_ORDERS);
        $this->dropTableIfExists(self::TABLE_USERS);

        parent::tearDown();
    }

    public function test_basic_select_all() : void
    {
        $query = select(star())->from(table(self::TABLE_USERS));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(3, $rows);
    }

    public function test_select_distinct() : void
    {
        $query = select()
            ->selectDistinct(col('status'))
            ->from(table(self::TABLE_ORDERS));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(2, $rows);
    }

    public function test_select_specific_columns() : void
    {
        $query = select(col('name'), col('email'))
            ->from(table(self::TABLE_USERS));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(3, $rows);
        self::assertArrayHasKey('name', $rows[0]);
        self::assertArrayHasKey('email', $rows[0]);
        self::assertArrayNotHasKey('id', $rows[0]);
    }

    public function test_select_with_cte() : void
    {
        $cteQuery = select(col('user_id'), agg_sum(col('amount'))->as('total'))
            ->from(table(self::TABLE_ORDERS))
            ->groupBy(col('user_id'));

        /** @phpstan-ignore method.notFound (WithBuilder::select return type issue - same as existing SelectBuilderTest) */
        $query = with(cte('order_totals', $cteQuery))
            ->select(star())
            ->from(table('order_totals'))
            ->where(gt(col('total'), literal(200)));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertGreaterThanOrEqual(1, \count($rows));
    }

    public function test_select_with_group_by_and_aggregate() : void
    {
        $query = select(col('user_id'), agg_sum(col('amount'))->as('total'))
            ->from(table(self::TABLE_ORDERS))
            ->groupBy(col('user_id'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(3, $rows);
    }

    public function test_select_with_join() : void
    {
        $query = select(col('name', self::TABLE_USERS), col('amount', self::TABLE_ORDERS))
            ->from(table(self::TABLE_USERS))
            ->join(
                table(self::TABLE_ORDERS),
                eq(col('id', self::TABLE_USERS), col('user_id', self::TABLE_ORDERS))
            );

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(4, $rows);
    }

    public function test_select_with_multiple_conditions() : void
    {
        $query = select(star())
            ->from(table(self::TABLE_USERS))
            ->where(
                cond_and(
                    gt(col('age'), literal(20)),
                    gt(col('age'), literal(25))
                )
            );

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(2, $rows);
    }

    public function test_select_with_order_by_and_limit() : void
    {
        $query = select(star())
            ->from(table(self::TABLE_USERS))
            ->orderBy(desc(col('age')))
            ->limit(2);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(2, $rows);
        self::assertSame('Bob Wilson', $rows[0]['name']);
        self::assertSame('John Doe', $rows[1]['name']);
    }

    public function test_select_with_where() : void
    {
        $query = select(star())
            ->from(table(self::TABLE_USERS))
            ->where(eq(col('name'), literal('John Doe')));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(1, $rows);
        self::assertSame('John Doe', $rows[0]['name']);
    }
}
