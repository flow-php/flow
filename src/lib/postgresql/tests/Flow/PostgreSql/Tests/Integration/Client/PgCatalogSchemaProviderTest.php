<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{
    agg_count,
    check_constraint,
    client_catalog_provider,
    col,
    column,
    column_type_bigint,
    column_type_bigserial,
    column_type_boolean,
    column_type_inet,
    column_type_integer,
    column_type_jsonb,
    column_type_serial,
    column_type_text,
    column_type_timestamp,
    column_type_timestamptz,
    column_type_uuid,
    column_type_varchar,
    create,
    eq,
    foreign_key,
    func,
    gt,
    index_col,
    literal,
    primary_key,
    select,
    table,
    type_attr,
    unique_constraint
};

use Flow\PostgreSql\QueryBuilder\Condition\OperatorCondition;
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnType, ReferentialAction};
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent as BuilderTriggerEvent;
use Flow\PostgreSql\Schema\{PartitionStrategy, TriggerEvent, TriggerTiming};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class PgCatalogSchemaProviderTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_provider_test';

    protected function setUp() : void
    {
        parent::setUp();

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
    }

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);

        parent::tearDown();
    }

    public function test_read_complex_schema() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(create()->enumType("{$s}.user_status")->labels('active', 'inactive', 'banned')->toSql());

        $this->pgsqlContext()->client()->execute(
            create()->compositeType("{$s}.address_type")
                ->attributes(
                    type_attr('street', ColumnType::text()),
                    type_attr('city', ColumnType::text()),
                    type_attr('zip', ColumnType::varchar(10)),
                )
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->domain("{$s}.positive_integer")
                ->as(column_type_integer())
                ->notNull()
                ->check(gt(col('value'), literal(0)))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->function("{$s}.update_modified_column")
                ->arguments()
                ->returns(ColumnType::custom('trigger'))
                ->language('plpgsql')
                ->as('BEGIN NEW.updated_at = now(); RETURN NEW; END;')
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table('users', $s)
                ->column(column('id', column_type_serial()))
                ->column(column('email', column_type_varchar(255))->notNull())
                ->column(column('name', column_type_text()))
                ->column(column('active', column_type_boolean())->notNull()->default(true))
                ->column(column('metadata', column_type_jsonb()))
                ->column(column('created_at', column_type_timestamptz())->defaultRaw(func('now')))
                ->constraint(primary_key('id'))
                ->constraint(unique_constraint('email'))
                ->constraint(check_constraint(new OperatorCondition(col('email'), '~*', literal('^.+@.+'))))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->index("{$s}.idx_users_active")
                ->on("{$s}.users")
                ->columns(index_col('email'))
                ->where(eq(col('active'), literal(true)))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table('posts', $s)
                ->column(column('id', column_type_bigserial()))
                ->column(column('user_id', column_type_integer())->notNull())
                ->column(column('title', column_type_text())->notNull())
                ->column(column('body', column_type_text()))
                ->column(column('external_id', column_type_uuid())->defaultRaw(func('gen_random_uuid')))
                ->column(column('updated_at', column_type_timestamptz()))
                ->constraint(primary_key('id'))
                ->constraint(foreign_key(['user_id'], "{$s}.users", ['id'])->onDelete(ReferentialAction::CASCADE))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->trigger('trg_posts_updated')
                ->before(BuilderTriggerEvent::UPDATE)
                ->on("{$s}.posts")
                ->forEachRow()
                ->execute("{$s}.update_modified_column")
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table('audit_log', $s)
                ->column(column('id', column_type_bigint())->identity())
                ->column(column('action', column_type_text())->notNull())
                ->column(column('ip_address', column_type_inet()))
                ->column(column('logged_at', column_type_timestamp())->defaultRaw(func('now')))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(create()->sequence('invoice_number_seq', $s)->startWith(1000)->incrementBy(1)->toSql());

        $this->pgsqlContext()->client()->execute(
            create()->view('active_users_view', $s)
                ->as(select(col('id'), col('email'))->from(table('users', $s)))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->materializedView('user_post_counts', $s)
                ->as(select(col('user_id'), agg_count()->as('post_count'))->from(table('posts', $s))->groupBy(col('user_id')))
                ->toSql()
        );

        $schema = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s);

        self::assertSame($s, $schema->name);

        self::assertTrue($schema->hasTable('users'));
        self::assertTrue($schema->hasTable('posts'));
        self::assertTrue($schema->hasTable('audit_log'));
        self::assertCount(3, $schema->tables);

        $users = $schema->table('users');
        self::assertFalse($users->column('id')->nullable);
        self::assertFalse($users->column('email')->nullable);
        self::assertTrue($users->column('name')->nullable);
        self::assertFalse($users->column('active')->nullable);
        self::assertNotNull($users->primaryKey);
        self::assertSame(['id'], $users->primaryKey->columns);
        self::assertCount(1, $users->uniqueConstraints);
        self::assertCount(1, $users->checkConstraints);

        $posts = $schema->table('posts');
        self::assertCount(1, $posts->foreignKeys);
        self::assertSame(['user_id'], $posts->foreignKeys[0]->columns);
        self::assertSame('users', $posts->foreignKeys[0]->referenceTable);
        self::assertSame(ReferentialAction::CASCADE, $posts->foreignKeys[0]->onDelete);

        self::assertCount(1, $posts->triggers);
        self::assertSame(TriggerTiming::BEFORE, $posts->triggers[0]->timing);
        self::assertContains(TriggerEvent::UPDATE, $posts->triggers[0]->events);
        self::assertTrue($posts->triggers[0]->forEachRow);

        self::assertTrue($schema->hasSequence('invoice_number_seq'));

        self::assertCount(1, $schema->views);
        self::assertSame('active_users_view', $schema->views[0]->name);

        self::assertCount(1, $schema->materializedViews);
        self::assertSame('user_post_counts', $schema->materializedViews[0]->name);

        $functionNames = \array_map(static fn ($f) => $f->name, $schema->functions);
        self::assertContains('update_modified_column', $functionNames);

        self::assertCount(1, $schema->domains);
        self::assertSame('positive_integer', $schema->domains[0]->name);
        self::assertFalse($schema->domains[0]->nullable);
        self::assertCount(1, $schema->domains[0]->checkConstraints);
    }

    public function test_read_non_partitioned_table_has_no_partition_info() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(
            create()->table('regular', $s)
                ->column(column('id', column_type_serial()))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s)->table('regular');

        self::assertNull($table->partitionStrategy);
        self::assertSame([], $table->partitionColumns);
        self::assertSame([], $table->inherits);
    }

    public function test_read_partitioned_table_by_hash() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(
            create()->table('distributed', $s)
                ->column(column('id', column_type_integer())->notNull())
                ->column(column('name', column_type_varchar(255)))
                ->partitionByHash('id')
                ->toSql()
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s)->table('distributed');

        self::assertSame(PartitionStrategy::HASH, $table->partitionStrategy);
        self::assertSame(['id'], $table->partitionColumns);
    }

    public function test_read_partitioned_table_by_list() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(
            create()->table('regional_data', $s)
                ->column(column('id', column_type_serial()))
                ->column(column('region', column_type_varchar(50))->notNull())
                ->partitionByList('region')
                ->toSql()
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s)->table('regional_data');

        self::assertSame(PartitionStrategy::LIST, $table->partitionStrategy);
        self::assertSame(['region'], $table->partitionColumns);
    }

    public function test_read_partitioned_table_by_range() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(
            create()->table('events', $s)
                ->column(column('id', column_type_serial()))
                ->column(column('created_at', column_type_timestamptz())->notNull())
                ->column(column('name', column_type_varchar(255)))
                ->partitionByRange('created_at')
                ->toSql()
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s)->table('events');

        self::assertSame(PartitionStrategy::RANGE, $table->partitionStrategy);
        self::assertSame(['created_at'], $table->partitionColumns);
    }

    public function test_read_single_table() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(
            create()->table('simple_table', $s)
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_varchar(100))->notNull())
                ->column(column('description', column_type_text()))
                ->column(column('is_active', column_type_boolean())->default(true))
                ->column(column('score', ColumnType::numeric(5, 2)))
                ->column(column('created_at', column_type_timestamptz())->defaultRaw(func('now')))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s)->table('simple_table');

        self::assertSame('simple_table', $table->name);
        self::assertCount(6, $table->columns);
        self::assertFalse($table->column('id')->nullable);
        self::assertFalse($table->column('name')->nullable);
        self::assertTrue($table->column('description')->nullable);
        self::assertNotNull($table->primaryKey);
        self::assertSame(['id'], $table->primaryKey->columns);
    }

    public function test_read_table_default_tablespace_is_null() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(
            create()->table('simple', $s)
                ->column(column('id', column_type_serial()))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s)->table('simple');

        self::assertNull($table->tablespace);
    }

    public function test_read_table_names() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(create()->table('alpha', $s)->column(column('id', column_type_serial()))->constraint(primary_key('id'))->toSql());
        $this->pgsqlContext()->client()->execute(create()->table('beta', $s)->column(column('id', column_type_serial()))->constraint(primary_key('id'))->toSql());
        $this->pgsqlContext()->client()->execute(create()->table('gamma', $s)->column(column('id', column_type_serial()))->constraint(primary_key('id'))->toSql());

        $schema = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s);

        self::assertSame(['alpha', 'beta', 'gamma'], $schema->tableNames());
    }

    public function test_read_table_with_inheritance() : void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute("SET search_path TO {$s}, public");

        $this->pgsqlContext()->client()->execute(
            create()->table('persons', $s)
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_varchar(255))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table('employees', $s)
                ->column(column('department', column_type_varchar(100)))
                ->inherits('persons')
                ->toSql()
        );

        $schema = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s);
        $employees = $schema->table('employees');

        self::assertSame(["{$s}.persons"], $employees->inherits);
    }

    public function test_schemas_returns_catalog_with_all_schemas_when_no_filter() : void
    {
        $catalog = client_catalog_provider($this->pgsqlContext()->client())->get();

        self::assertTrue($catalog->has(self::SCHEMA));
        self::assertContains(self::SCHEMA, $catalog->names());
        self::assertNotContains('pg_catalog', $catalog->names());
        self::assertNotContains('information_schema', $catalog->names());
    }

    public function test_schemas_returns_only_filtered_schemas() : void
    {
        $catalog = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])->get();

        self::assertSame([self::SCHEMA], $catalog->names());
        self::assertCount(1, $catalog->all());
    }
}
