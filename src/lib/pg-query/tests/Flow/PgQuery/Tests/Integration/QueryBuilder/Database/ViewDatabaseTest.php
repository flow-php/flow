<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Database;

use function Flow\PgQuery\DSL\{
    col,
    column,
    create_materialized_view,
    create_table,
    create_view,
    drop_materialized_view,
    drop_view,
    eq,
    gt,
    insert,
    literal_int,
    literal_string,
    refresh_materialized_view,
    select,
    sql_type_integer,
    sql_type_serial,
    sql_type_varchar,
    star,
    table
};

final class ViewDatabaseTest extends DatabaseTestCase
{
    private const MATVIEW_SIMPLE = 'flow_postgres_simple_matview';

    private const TABLE_SOURCE = 'flow_postgres_view_source';

    private const VIEW_FILTERED = 'flow_postgres_filtered_view';

    private const VIEW_SIMPLE = 'flow_postgres_simple_view';

    protected function setUp() : void
    {
        parent::setUp();

        $query = create_table(self::TABLE_SOURCE)
            ->column(column('id', sql_type_serial()))
            ->column(column('name', sql_type_varchar(100)))
            ->column(column('value', sql_type_integer()));

        $this->execute($query->toSql());

        $this->execute(
            insert()
                ->into(self::TABLE_SOURCE)
                ->columns('name', 'value')
                ->values(literal_string('Item A'), literal_int(100))
                ->values(literal_string('Item B'), literal_int(200))
                ->values(literal_string('Item C'), literal_int(50))
                ->values(literal_string('Item D'), literal_int(300))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropViewIfExists(self::VIEW_SIMPLE);
        $this->dropViewIfExists(self::VIEW_FILTERED);
        $this->dropMaterializedViewIfExists(self::MATVIEW_SIMPLE);
        $this->dropTableIfExists(self::TABLE_SOURCE);

        parent::tearDown();
    }

    public function test_create_materialized_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $query = create_materialized_view(self::MATVIEW_SIMPLE)
            ->as($selectQuery);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("SELECT matviewname FROM pg_matviews WHERE matviewname = '" . self::MATVIEW_SIMPLE . "'");
        $views = $this->fetchAll($check);
        self::assertCount(1, $views);
    }

    public function test_create_or_replace_view() : void
    {
        $selectQuery1 = select(col('id'), col('name'))->from(table(self::TABLE_SOURCE));
        $createQuery1 = create_view(self::VIEW_SIMPLE)->as($selectQuery1);
        $this->execute($createQuery1->toSql());

        $selectQuery2 = select(col('id'), col('name'), col('value'))->from(table(self::TABLE_SOURCE));
        $replaceQuery = create_view(self::VIEW_SIMPLE)
            ->orReplace()
            ->as($selectQuery2);

        $result = $this->execute($replaceQuery->toSql());

        self::assertNotFalse($result);

        $check = $this->execute('SELECT * FROM ' . self::VIEW_SIMPLE);
        $row = $this->fetchOne($check);
        self::assertArrayHasKey('id', $row);
        self::assertArrayHasKey('name', $row);
        self::assertArrayHasKey('value', $row);
    }

    public function test_create_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $query = create_view(self::VIEW_SIMPLE)
            ->as($selectQuery);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute(
            select(col('table_name'))
                ->from(table('information_schema.views'))
                ->where(eq(col('table_name'), literal_string(self::VIEW_SIMPLE)))
                ->toSql()
        );
        $views = $this->fetchAll($check);
        self::assertCount(1, $views);
    }

    public function test_create_view_with_filter() : void
    {
        $selectQuery = select(star())
            ->from(table(self::TABLE_SOURCE))
            ->where(gt(col('value'), literal_int(100)));

        $query = create_view(self::VIEW_FILTERED)
            ->as($selectQuery);

        $this->execute($query->toSql());

        $result = $this->execute('SELECT * FROM ' . self::VIEW_FILTERED);

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(2, $rows);
    }

    public function test_drop_materialized_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));
        $createQuery = create_materialized_view(self::MATVIEW_SIMPLE)->as($selectQuery);
        $this->execute($createQuery->toSql());

        $dropQuery = drop_materialized_view(self::MATVIEW_SIMPLE);

        $result = $this->execute($dropQuery->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("SELECT matviewname FROM pg_matviews WHERE matviewname = '" . self::MATVIEW_SIMPLE . "'");
        $views = $this->fetchAll($check);
        self::assertCount(0, $views);
    }

    public function test_drop_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));
        $createQuery = create_view(self::VIEW_SIMPLE)->as($selectQuery);
        $this->execute($createQuery->toSql());

        $dropQuery = drop_view(self::VIEW_SIMPLE);

        $result = $this->execute($dropQuery->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("SELECT table_name FROM information_schema.views WHERE table_name = '" . self::VIEW_SIMPLE . "'");
        $views = $this->fetchAll($check);
        self::assertCount(0, $views);
    }

    public function test_drop_view_if_exists() : void
    {
        $dropQuery = drop_view(self::VIEW_SIMPLE)->ifExists();

        $result = $this->execute($dropQuery->toSql());

        self::assertNotFalse($result);
    }

    public function test_materialized_view_returns_data() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $createQuery = create_materialized_view(self::MATVIEW_SIMPLE)
            ->as($selectQuery);
        $this->execute($createQuery->toSql());

        $result = $this->execute('SELECT * FROM ' . self::MATVIEW_SIMPLE);

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(4, $rows);
    }

    public function test_refresh_materialized_view() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $createQuery = create_materialized_view(self::MATVIEW_SIMPLE)
            ->as($selectQuery);
        $this->execute($createQuery->toSql());

        $this->execute('INSERT INTO ' . self::TABLE_SOURCE . " (name, value) VALUES ('Item E', 500)");

        $beforeRefresh = $this->execute('SELECT COUNT(*) as cnt FROM ' . self::MATVIEW_SIMPLE);
        $beforeRow = $this->fetchOne($beforeRefresh);
        self::assertSame('4', $beforeRow['cnt']);

        $refreshQuery = refresh_materialized_view(self::MATVIEW_SIMPLE);
        $this->execute($refreshQuery->toSql());

        $afterRefresh = $this->execute('SELECT COUNT(*) as cnt FROM ' . self::MATVIEW_SIMPLE);
        $afterRow = $this->fetchOne($afterRefresh);
        self::assertSame('5', $afterRow['cnt']);
    }

    public function test_view_returns_data() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_SOURCE));

        $createQuery = create_view(self::VIEW_SIMPLE)
            ->as($selectQuery);
        $this->execute($createQuery->toSql());

        $result = $this->execute(
            select(star())->from(table(self::VIEW_SIMPLE))->toSql()
        );

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(4, $rows);
    }
}
