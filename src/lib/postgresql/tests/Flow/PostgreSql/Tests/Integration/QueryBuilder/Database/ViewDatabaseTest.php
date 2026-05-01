<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    agg_count,
    col,
    column,
    column_type_integer,
    column_type_serial,
    column_type_varchar,
    create,
    drop,
    eq,
    gt,
    insert,
    literal,
    refresh_materialized_view,
    select,
    star,
    table
};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class ViewDatabaseTest extends PostgreSqlTestCase
{
    private const MATVIEW_SIMPLE = 'flow_postgres_simple_matview';

    private const TABLE_SOURCE = 'flow_postgres_view_source';

    private const VIEW_FILTERED = 'flow_postgres_filtered_view';

    private const VIEW_SIMPLE = 'flow_postgres_simple_view';

    protected function setUp() : void
    {
        parent::setUp();

        $query = create()->table(self::TABLE_SOURCE)
            ->column(column('id', column_type_serial()))
            ->column(column('name', column_type_varchar(100)))
            ->column(column('value', column_type_integer()));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_SOURCE)
                ->columns('name', 'value')
                ->values(literal('Item A'), literal(100))
                ->values(literal('Item B'), literal(200))
                ->values(literal('Item C'), literal(50))
                ->values(literal('Item D'), literal(300))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropViewIfExists(self::VIEW_SIMPLE);
        $this->pgsqlContext()->dropViewIfExists(self::VIEW_FILTERED);
        $this->pgsqlContext()->dropMaterializedViewIfExists(self::MATVIEW_SIMPLE);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_SOURCE);

        parent::tearDown();
    }

    public function test_create_materialized_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $query = create()->materializedView(self::MATVIEW_SIMPLE)
            ->as($selectQuery);

        $this->pgsqlContext()->client()->execute($query->toSql());

        $views = $this->pgsqlContext()->client()->fetchAll(
            select(col('matviewname'))
                ->from(table('pg_matviews'))
                ->where(eq(col('matviewname'), literal(self::MATVIEW_SIMPLE)))
                ->toSql()
        );
        self::assertCount(1, $views);
    }

    public function test_create_or_replace_view() : void
    {
        $selectQuery1 = select(col('id'), col('name'))->from(table(self::TABLE_SOURCE));
        $createQuery1 = create()->view(self::VIEW_SIMPLE)->as($selectQuery1);
        $this->pgsqlContext()->client()->execute($createQuery1->toSql());

        $selectQuery2 = select(col('id'), col('name'), col('value'))->from(table(self::TABLE_SOURCE));
        $replaceQuery = create()->view(self::VIEW_SIMPLE)
            ->orReplace()
            ->as($selectQuery2);

        $this->pgsqlContext()->client()->execute($replaceQuery->toSql());

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::VIEW_SIMPLE))->toSql()
        );
        self::assertNotEmpty($rows);
        self::assertArrayHasKey('id', $rows[0]);
        self::assertArrayHasKey('name', $rows[0]);
        self::assertArrayHasKey('value', $rows[0]);
    }

    public function test_create_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $query = create()->view(self::VIEW_SIMPLE)
            ->as($selectQuery);

        $this->pgsqlContext()->client()->execute($query->toSql());

        $views = $this->pgsqlContext()->client()->fetchAll(
            select(col('table_name'))
                ->from(table('information_schema.views'))
                ->where(eq(col('table_name'), literal(self::VIEW_SIMPLE)))
                ->toSql()
        );
        self::assertCount(1, $views);
    }

    public function test_create_view_with_filter() : void
    {
        $selectQuery = select(star())
            ->from(table(self::TABLE_SOURCE))
            ->where(gt(col('value'), literal(100)));

        $query = create()->view(self::VIEW_FILTERED)
            ->as($selectQuery);

        $this->pgsqlContext()->client()->execute($query->toSql());

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::VIEW_FILTERED))->toSql()
        );
        self::assertCount(2, $rows);
    }

    public function test_drop_materialized_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));
        $createQuery = create()->materializedView(self::MATVIEW_SIMPLE)->as($selectQuery);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $dropQuery = drop()->materializedView(self::MATVIEW_SIMPLE);

        $this->pgsqlContext()->client()->execute($dropQuery->toSql());

        $views = $this->pgsqlContext()->client()->fetchAll(
            select(col('matviewname'))
                ->from(table('pg_matviews'))
                ->where(eq(col('matviewname'), literal(self::MATVIEW_SIMPLE)))
                ->toSql()
        );
        self::assertCount(0, $views);
    }

    public function test_drop_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));
        $createQuery = create()->view(self::VIEW_SIMPLE)->as($selectQuery);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $dropQuery = drop()->view(self::VIEW_SIMPLE);

        $this->pgsqlContext()->client()->execute($dropQuery->toSql());

        $views = $this->pgsqlContext()->client()->fetchAll(
            select(col('table_name'))
                ->from(table('information_schema.views'))
                ->where(eq(col('table_name'), literal(self::VIEW_SIMPLE)))
                ->toSql()
        );
        self::assertCount(0, $views);
    }

    public function test_drop_view_if_exists() : void
    {
        $this->expectNotToPerformAssertions();

        $dropQuery = drop()->view(self::VIEW_SIMPLE)->ifExists();

        $this->pgsqlContext()->client()->execute($dropQuery->toSql());
    }

    public function test_materialized_view_returns_data() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $createQuery = create()->materializedView(self::MATVIEW_SIMPLE)
            ->as($selectQuery);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::MATVIEW_SIMPLE))->toSql()
        );
        self::assertCount(4, $rows);
    }

    public function test_refresh_materialized_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $createQuery = create()->materializedView(self::MATVIEW_SIMPLE)
            ->as($selectQuery);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_SOURCE)
                ->columns('name', 'value')
                ->values(literal('Item E'), literal(500))
                ->toSql()
        );

        $beforeRow = $this->pgsqlContext()->client()->fetchSingle(
            select(agg_count()->as('cnt'))->from(table(self::MATVIEW_SIMPLE))->toSql()
        );
        self::assertSame(4, $beforeRow['cnt']);

        $refreshQuery = refresh_materialized_view(self::MATVIEW_SIMPLE);
        $this->pgsqlContext()->client()->execute($refreshQuery->toSql());

        $afterRow = $this->pgsqlContext()->client()->fetchSingle(
            select(agg_count()->as('cnt'))->from(table(self::MATVIEW_SIMPLE))->toSql()
        );
        self::assertSame(5, $afterRow['cnt']);
    }

    public function test_view_returns_data() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $createQuery = create()->view(self::VIEW_SIMPLE)
            ->as($selectQuery);
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::VIEW_SIMPLE))->toSql()
        );
        self::assertCount(4, $rows);
    }
}
