<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function count;
use function Flow\PostgreSql\DSL\agg_count;
use function Flow\PostgreSql\DSL\agg_sum;
use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\binary_expr;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_decimal;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\cte;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\desc;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\gt;
use function Flow\PostgreSql\DSL\in_subquery;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\is_true;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\lt;
use function Flow\PostgreSql\DSL\merge;
use function Flow\PostgreSql\DSL\merge_action;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\with;

final class SelectDatabaseTest extends PostgreSqlTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_select_schema';

    private const SCHEMA_TABLE = 'flow_postgres_schema_users';

    private const TABLE_ORDERS = 'flow_postgres_orders';

    private const TABLE_USERS = 'flow_postgres_users';

    protected function setUp(): void
    {
        parent::setUp();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_USERS)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->column(column('email', column_type_varchar(255)))
                    ->column(column('age', column_type_integer())->default(0))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_ORDERS)
                    ->column(column('id', column_type_serial()))
                    ->column(column('user_id', column_type_integer()))
                    ->column(column('amount', column_type_decimal(10, 2))->notNull())
                    ->column(column('status', column_type_varchar(50))->default('pending'))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_USERS)
                    ->columns('name', 'email', 'age')
                    ->values(literal('John Doe'), literal('john@example.com'), literal(30))
                    ->values(literal('Jane Smith'), literal('jane@example.com'), literal(25))
                    ->values(literal('Bob Wilson'), literal('bob@example.com'), literal(35))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_ORDERS)
                    ->columns('user_id', 'amount', 'status')
                    ->values(literal(1), literal(100), literal('completed'))
                    ->values(literal(1), literal(200), literal('pending'))
                    ->values(literal(2), literal(150), literal('completed'))
                    ->values(literal(3), literal(300), literal('completed'))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_ORDERS);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_USERS);
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME);

        parent::tearDown();
    }

    public function test_basic_select_all(): void
    {
        $query = select(star())->from(table(self::TABLE_USERS));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(3, $rows);
    }

    public function test_select_distinct(): void
    {
        $query = select()->selectDistinct(col('status'))->from(table(self::TABLE_ORDERS));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(2, $rows);
    }

    public function test_select_specific_columns(): void
    {
        $query = select(col('name'), col('email'))->from(table(self::TABLE_USERS));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(3, $rows);
        static::assertArrayHasKey('name', $rows[0]);
        static::assertArrayHasKey('email', $rows[0]);
        static::assertArrayNotHasKey('id', $rows[0]);
    }

    public function test_select_reads_returning_of_merge_cte(): void
    {
        static::assertSame(
            [['merge_action' => 'DELETE', 'id' => 2]],
            $this
                ->pgsqlContext()
                ->client()
                ->fetchAll(
                    with(cte('m', merge(self::TABLE_ORDERS, 'o')
                        ->using(self::TABLE_USERS, 'u')
                        ->on(eq(col('user_id', 'o'), col('id', 'u')))
                        ->whenMatchedAnd(eq(col('status', 'o'), literal('pending')))
                        ->thenDelete()
                        ->returning(merge_action(), col('id', 'o'))))
                        ->select(star())
                        ->from(table('m'))
                        ->toSql(),
                ),
        );
    }

    public function test_select_reads_returning_of_data_modifying_cte(): void
    {
        static::assertSame(
            [['id' => 2, 'user_id' => 1]],
            $this
                ->pgsqlContext()
                ->client()
                ->fetchAll(
                    with(cte('removed_orders', delete()
                        ->from(self::TABLE_ORDERS)
                        ->where(eq(col('status'), literal('pending')))
                        ->returning(col('id'), col('user_id'))))
                        ->select(col('id'), col('user_id'))
                        ->from(table('removed_orders'))
                        ->toSql(),
                ),
        );
        static::assertSame(
            3,
            $this
                ->pgsqlContext()
                ->client()
                ->fetchSingle(select(agg_count(star())->as('cnt'))->from(table(self::TABLE_ORDERS))->toSql())['cnt'],
        );
    }

    public function test_select_from_cte_with_cycle_stops_on_cyclic_graph(): void
    {
        static::assertSame(
            [['id' => 1, 'path' => ['(1)', '(2)', '(3)', '(1)']]],
            $this
                ->pgsqlContext()
                ->client()
                ->fetchAll(
                    with(
                        cte(
                            'edges',
                            select(literal(1)->as('id'), literal(2)->as('link'))
                                ->unionAll(select(literal(2), literal(3)))
                                ->unionAll(select(literal(3), literal(1))),
                        ),
                        cte(
                            't',
                            select(col('id'), col('link'))
                                ->from(table('edges'))
                                ->where(eq(col('id'), literal(1)))
                                ->unionAll(
                                    select(col('id', 'e'), col('link', 'e'))
                                        ->from(table('edges')->as('e'))
                                        ->join(table('t'), eq(col('id', 'e'), col('link', 't'))),
                                ),
                            ['id', 'link'],
                        )->cycle(['id'], 'is_cycle', 'path'),
                    )
                        ->recursive()
                        ->select(col('id'), col('path'))
                        ->from(table('t'))
                        ->where(is_true(col('is_cycle')))
                        ->toSql(),
                ),
        );
    }

    public function test_select_from_cte_with_search_breadth_first_orders_by_sequence_column(): void
    {
        static::assertSame(
            [['n' => 3], ['n' => 2], ['n' => 1]],
            $this
                ->pgsqlContext()
                ->client()
                ->fetchAll(
                    with(cte(
                        't',
                        select(literal(1))
                            ->unionAll(
                                select(binary_expr(col('n'), '+', literal(1)))
                                    ->from(table('t'))
                                    ->where(lt(col('n'), literal(3))),
                            ),
                        ['n'],
                    )->searchBreadthFirst(['n'], 'ord'))
                        ->recursive()
                        ->select(col('n'))
                        ->from(table('t'))
                        ->orderBy(desc(col('ord')))
                        ->toSql(),
                ),
        );
    }

    public function test_select_with_cte(): void
    {
        $cteQuery = select(col('user_id'), agg_sum(col('amount'))->as('total'))
            ->from(table(self::TABLE_ORDERS))
            ->groupBy(col('user_id'));

        $query = with(cte('order_totals', $cteQuery))
            ->select(star())
            ->from(table('order_totals'))
            ->where(gt(col('total'), literal(200)));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertGreaterThanOrEqual(1, count($rows));
    }

    public function test_chained_union_all_returns_all_branches(): void
    {
        $query = select(col('name'))
            ->from(table(self::TABLE_USERS))
            ->where(eq(col('age'), literal(30)))
            ->unionAll(select(col('name'))->from(table(self::TABLE_USERS))->where(eq(col('age'), literal(25))))
            ->unionAll(select(col('name'))->from(table(self::TABLE_USERS))->where(eq(col('age'), literal(35))));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(3, $rows);
    }

    public function test_select_with_group_by_and_aggregate(): void
    {
        $query = select(col('user_id'), agg_sum(col('amount'))->as('total'))
            ->from(table(self::TABLE_ORDERS))
            ->groupBy(col('user_id'));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(3, $rows);
    }

    public function test_select_with_join(): void
    {
        $query = select(col('name', self::TABLE_USERS), col('amount', self::TABLE_ORDERS))
            ->from(table(self::TABLE_USERS))
            ->join(table(self::TABLE_ORDERS), eq(col('id', self::TABLE_USERS), col('user_id', self::TABLE_ORDERS)));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(4, $rows);
    }

    public function test_select_with_multiple_conditions(): void
    {
        $query = select(star())
            ->from(table(self::TABLE_USERS))
            ->where(and_(gt(col('age'), literal(20)), gt(col('age'), literal(25))));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(2, $rows);
    }

    public function test_select_with_order_by_and_limit(): void
    {
        $query = select(star())
            ->from(table(self::TABLE_USERS))
            ->orderBy(desc(col('age')))
            ->limit(2);

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(2, $rows);
        static::assertSame('Bob Wilson', $rows[0]['name']);
        static::assertSame('John Doe', $rows[1]['name']);
    }

    public function test_select_with_schema_qualified_table(): void
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
                    ->column(column('email', column_type_varchar(255)))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)
                    ->columns('name', 'email')
                    ->values(literal('Schema User'), literal('schema@example.com'))
                    ->toSql(),
            );

        $query = select(star())->from(table(self::SCHEMA_TABLE, self::SCHEMA_NAME));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(1, $rows);
        static::assertSame('Schema User', $rows[0]['name']);
        static::assertSame('schema@example.com', $rows[0]['email']);
    }

    public function test_select_with_in_subquery(): void
    {
        static::assertSame(
            [['name' => 'John Doe']],
            $this
                ->pgsqlContext()
                ->client()
                ->fetchAll(
                    select(col('name'))
                        ->from(table(self::TABLE_USERS))
                        ->where(in_subquery(
                            'id',
                            select(col('user_id'))
                                ->from(table(self::TABLE_ORDERS))
                                ->where(eq(col('status'), literal('pending'))),
                        ))
                        ->toSql(),
                ),
        );
    }

    public function test_select_with_where(): void
    {
        $query = select(star())->from(table(self::TABLE_USERS))->where(eq(col('name'), literal('John Doe')));

        $rows = $this->pgsqlContext()->client()->fetchAll($query->toSql());

        static::assertCount(1, $rows);
        static::assertSame('John Doe', $rows[0]['name']);
    }
}
