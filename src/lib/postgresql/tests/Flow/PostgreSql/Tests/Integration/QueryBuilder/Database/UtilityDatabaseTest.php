<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;
use Flow\PostgreSql\QueryBuilder\Utility\LockMode;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\analyze;
use function Flow\PostgreSql\DSL\cluster;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\comment;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\explain;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\lock_table;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\vacuum;

final class UtilityDatabaseTest extends PostgreSqlTestCase
{
    private const INDEX_NAME = 'flow_postgres_utility_idx';

    private const TABLE_NAME = 'flow_postgres_utility_test';

    protected function setUp(): void
    {
        parent::setUp();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_NAME)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->column(column('value', column_type_integer())->default(0))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_NAME)
                    ->columns('name', 'value')
                    ->values(literal('Alice'), literal(100))
                    ->values(literal('Bob'), literal(200))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropIndexIfExists(self::INDEX_NAME);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_NAME);

        parent::tearDown();
    }

    public function test_analyze_specific_columns(): void
    {
        $this->expectNotToPerformAssertions();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(analyze()->table(self::TABLE_NAME, 'name', 'value')->toSql());
    }

    public function test_analyze_table(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(analyze()->tables(self::TABLE_NAME)->toSql());
    }

    public function test_analyze_verbose(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(analyze()->verbose()->tables(self::TABLE_NAME)->toSql());
    }

    public function test_cluster_table_on_index(): void
    {
        $this->expectNotToPerformAssertions();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->index(self::INDEX_NAME)->on(self::TABLE_NAME)->columns('name')->toSql());

        $this->pgsqlContext()->client()->execute(cluster()->table(self::TABLE_NAME)->using(self::INDEX_NAME)->toSql());
    }

    public function test_cluster_verbose(): void
    {
        $this->expectNotToPerformAssertions();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(create()->index(self::INDEX_NAME)->on(self::TABLE_NAME)->columns('name')->toSql());

        $this->pgsqlContext()->client()->execute(cluster()->table(self::TABLE_NAME)->using(self::INDEX_NAME)->toSql());

        $this->pgsqlContext()->client()->execute(cluster()->verbose()->table(self::TABLE_NAME)->toSql());
    }

    public function test_comment_on_column(): void
    {
        $this->expectNotToPerformAssertions();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(comment(CommentTarget::COLUMN, self::TABLE_NAME . '.name')->is('Name of the entity')->toSql());
    }

    public function test_comment_on_table(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                comment(CommentTarget::TABLE, self::TABLE_NAME)->is('This is a test table for utility tests')->toSql(),
            );

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle("SELECT obj_description('" . self::TABLE_NAME . "'::regclass, 'pg_class') AS comment");

        static::assertSame('This is a test table for utility tests', $row['comment']);
    }

    public function test_comment_remove(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(comment(CommentTarget::TABLE, self::TABLE_NAME)->is('Some comment')->toSql());

        $this->pgsqlContext()->client()->execute(comment(CommentTarget::TABLE, self::TABLE_NAME)->isNull()->toSql());

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle("SELECT obj_description('" . self::TABLE_NAME . "'::regclass, 'pg_class') AS comment");

        static::assertNull($row['comment']);
    }

    public function test_explain_analyze(): void
    {
        $selectQuery = select(star())->from(table(self::TABLE_NAME));

        $rows = $this->pgsqlContext()->client()->fetchAll(explain($selectQuery)->analyze()->toSql());
        static::assertNotEmpty($rows);
    }

    public function test_explain_select(): void
    {
        $selectQuery = select(star())->from(table(self::TABLE_NAME));

        $rows = $this->pgsqlContext()->client()->fetchAll(explain($selectQuery)->toSql());
        static::assertNotEmpty($rows);
        static::assertArrayHasKey('QUERY PLAN', $rows[0]);
    }

    public function test_explain_verbose(): void
    {
        $selectQuery = select(star())->from(table(self::TABLE_NAME));

        $rows = $this->pgsqlContext()->client()->fetchAll(explain($selectQuery)->verbose()->toSql());
        static::assertNotEmpty($rows);
    }

    public function test_lock_table(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute('BEGIN');

        $this->pgsqlContext()->client()->execute(lock_table(self::TABLE_NAME)->toSql());

        $this->pgsqlContext()->client()->execute('COMMIT');
    }

    public function test_lock_table_nowait(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute('BEGIN');

        $this->pgsqlContext()->client()->execute(lock_table(self::TABLE_NAME)->noWait()->toSql());

        $this->pgsqlContext()->client()->execute('COMMIT');
    }

    public function test_lock_table_with_mode(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute('BEGIN');

        $this->pgsqlContext()->client()->execute(lock_table(self::TABLE_NAME)->inMode(LockMode::SHARE)->toSql());

        $this->pgsqlContext()->client()->execute('COMMIT');
    }

    public function test_vacuum_analyze_table(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(vacuum()->analyze()->tables(self::TABLE_NAME)->toSql());
    }

    public function test_vacuum_full(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(vacuum()->full()->tables(self::TABLE_NAME)->toSql());
    }

    public function test_vacuum_table(): void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(vacuum()->tables(self::TABLE_NAME)->toSql());
    }
}
