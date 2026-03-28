<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\{
    agg_count,
    col,
    column_type_array,
    column_type_custom,
    column_type_integer,
    eq,
    literal,
    schema,
    schema_check,
    schema_column,
    schema_column_big_serial,
    schema_column_bigint,
    schema_column_boolean,
    schema_column_bytea,
    schema_column_char,
    schema_column_cidr,
    schema_column_date,
    schema_column_double_precision,
    schema_column_inet,
    schema_column_integer,
    schema_column_interval,
    schema_column_json,
    schema_column_jsonb,
    schema_column_macaddr,
    schema_column_numeric,
    schema_column_real,
    schema_column_serial,
    schema_column_small_serial,
    schema_column_smallint,
    schema_column_text,
    schema_column_time,
    schema_column_timestamp,
    schema_column_timestamp_tz,
    schema_column_uuid,
    schema_column_varchar,
    schema_domain,
    schema_exclude,
    schema_extension,
    schema_foreign_key,
    schema_function,
    schema_index,
    schema_materialized_view,
    schema_primary_key,
    schema_procedure,
    schema_sequence,
    schema_table,
    schema_trigger,
    schema_unique,
    schema_view,
    select,
    star,
    table,
};
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use Flow\PostgreSql\Schema\Exception\{SchemaException, TableNotFoundException};
use Flow\PostgreSql\Schema\{FunctionVolatility, IndexMethod, TriggerEvent, TriggerTiming};

use PHPUnit\Framework\TestCase;

final class SchemaTest extends TestCase
{
    public function test_comprehensive_schema_with_all_object_types() : void
    {
        $schema = schema(
            'app',
            tables: [
                schema_table(
                    'users',
                    [
                        schema_column_serial('id'),
                        schema_column_varchar('email', 255, nullable: false),
                        schema_column_text('name'),
                        schema_column_boolean('active', default: 'true'),
                        schema_column_jsonb('metadata'),
                        schema_column_timestamp_tz('created_at', default: 'now()'),
                        schema_column_timestamp_tz('updated_at'),
                    ],
                    primaryKey: schema_primary_key(['id'], 'users_pkey'),
                    indexes: [
                        schema_index('idx_users_email', ['email'], unique: true),
                        schema_index('idx_users_metadata', ['metadata'], method: IndexMethod::GIN),
                        schema_index('idx_users_active', ['email'], predicate: 'active = true'),
                    ],
                    uniqueConstraints: [schema_unique(['email'], 'uq_users_email')],
                    checkConstraints: [schema_check("email ~* '^.+@.+$'", 'chk_users_email_format')],
                    schema: 'app',
                ),
                schema_table(
                    'posts',
                    [
                        schema_column_big_serial('id'),
                        schema_column_integer('user_id', nullable: false),
                        schema_column_text('title', nullable: false),
                        schema_column_text('body'),
                        schema_column_uuid('external_id', default: 'gen_random_uuid()'),
                        schema_column_timestamp_tz('published_at'),
                    ],
                    primaryKey: schema_primary_key(['id']),
                    foreignKeys: [
                        schema_foreign_key(
                            ['user_id'],
                            'users',
                            ['id'],
                            name: 'fk_posts_user_id',
                            referenceSchema: 'app',
                            onUpdate: ReferentialAction::NO_ACTION,
                            onDelete: ReferentialAction::CASCADE,
                        ),
                    ],
                    triggers: [
                        schema_trigger(
                            'trg_posts_updated',
                            'posts',
                            TriggerTiming::BEFORE,
                            [TriggerEvent::UPDATE],
                            'update_timestamp',
                            forEachRow: true,
                        ),
                    ],
                    schema: 'app',
                ),
                schema_table(
                    'audit_log',
                    [
                        schema_column_bigint('id', nullable: false),
                        schema_column_text('action', nullable: false),
                        schema_column_inet('ip_address'),
                        schema_column_bytea('payload'),
                        schema_column_timestamp('logged_at'),
                    ],
                    excludeConstraints: [schema_exclude('USING gist (tsrange WITH &&)', 'excl_audit_overlap')],
                    schema: 'app',
                ),
            ],
            sequences: [
                schema_sequence('invoice_number_seq', dataType: 'integer', startValue: 1000, incrementBy: 1),
            ],
            views: [
                schema_view('active_users', select(col('id'), col('email'))->from(table('users', 'app'))->where(eq(col('active'), literal(true)))->toSql()),
            ],
            materializedViews: [
                schema_materialized_view(
                    'user_post_counts',
                    select(col('user_id'), agg_count()->as('post_count'))->from(table('posts', 'app'))->groupBy(col('user_id'))->toSql(),
                    indexes: [schema_index('idx_upc_user_id', ['user_id'], unique: true)],
                ),
            ],
            functions: [
                schema_function(
                    'update_timestamp',
                    'trigger',
                    language: 'plpgsql',
                    definition: 'BEGIN NEW.updated_at = now(); RETURN NEW; END;',
                    volatility: FunctionVolatility::VOLATILE,
                ),
            ],
            procedures: [
                schema_procedure(
                    'cleanup_old_posts',
                    argumentTypes: ['integer'],
                    language: 'plpgsql',
                    definition: 'BEGIN DELETE FROM app.posts WHERE published_at < now() - make_interval(days => $1); END;',
                ),
            ],
            domains: [
                schema_domain(
                    'positive_int',
                    column_type_integer(),
                    nullable: false,
                    checkConstraints: [schema_check('VALUE > 0', 'chk_positive')],
                ),
            ],
            extensions: [
                schema_extension('pgcrypto', '1.3'),
            ],
        );

        self::assertSame('app', $schema->name);
        self::assertCount(3, $schema->tables);
        self::assertCount(1, $schema->sequences);
        self::assertCount(1, $schema->views);
        self::assertCount(1, $schema->materializedViews);
        self::assertCount(1, $schema->functions);
        self::assertCount(1, $schema->procedures);
        self::assertCount(1, $schema->domains);
        self::assertCount(1, $schema->extensions);

        $usersTable = $schema->table('users');
        self::assertCount(7, $usersTable->columns);
        self::assertNotNull($usersTable->primaryKey);
        self::assertCount(3, $usersTable->indexes);
        self::assertCount(1, $usersTable->uniqueConstraints);
        self::assertCount(1, $usersTable->checkConstraints);

        $postsTable = $schema->table('posts');
        self::assertCount(1, $postsTable->foreignKeys);
        self::assertSame(ReferentialAction::CASCADE, $postsTable->foreignKeys[0]->onDelete);
        self::assertCount(1, $postsTable->triggers);
        self::assertTrue($postsTable->triggers[0]->forEachRow);

        $auditTable = $schema->table('audit_log');
        self::assertCount(1, $auditTable->excludeConstraints);
    }

    public function test_has_table() : void
    {
        $schema = schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ]);

        self::assertTrue($schema->hasTable('users'));
        self::assertFalse($schema->hasTable('missing'));
    }

    public function test_schema_column_type_shortcuts_all_types() : void
    {
        $table = schema_table('all_types', [
            schema_column_integer('col_integer'),
            schema_column_smallint('col_smallint'),
            schema_column_bigint('col_bigint'),
            schema_column_serial('col_serial'),
            schema_column_small_serial('col_smallserial'),
            schema_column_big_serial('col_bigserial'),
            schema_column_boolean('col_boolean'),
            schema_column_text('col_text'),
            schema_column_varchar('col_varchar', 100),
            schema_column_char('col_char', 5),
            schema_column_numeric('col_numeric', 10, 2),
            schema_column_real('col_real'),
            schema_column_double_precision('col_double'),
            schema_column_date('col_date'),
            schema_column_time('col_time', 3),
            schema_column_timestamp('col_timestamp'),
            schema_column_timestamp_tz('col_timestamptz', 6),
            schema_column_interval('col_interval'),
            schema_column_uuid('col_uuid'),
            schema_column_json('col_json'),
            schema_column_jsonb('col_jsonb'),
            schema_column_bytea('col_bytea'),
            schema_column_inet('col_inet'),
            schema_column_cidr('col_cidr'),
            schema_column_macaddr('col_macaddr'),
            schema_column('col_array', column_type_array(column_type_integer())),
            schema_column('col_custom', column_type_custom('hstore')),
        ]);

        self::assertCount(27, $table->columns);
        self::assertFalse($table->column('col_serial')->nullable);
        self::assertFalse($table->column('col_smallserial')->nullable);
        self::assertFalse($table->column('col_bigserial')->nullable);
        self::assertTrue($table->column('col_text')->nullable);
    }

    public function test_schema_construction() : void
    {
        $schema = schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ]);

        self::assertSame('public', $schema->name);
        self::assertCount(1, $schema->tables);
    }

    public function test_schema_empty() : void
    {
        $s = schema('public');

        self::assertSame([], $s->tables);
        self::assertSame([], $s->sequences);
        self::assertSame([], $s->views);
        self::assertSame([], $s->materializedViews);
        self::assertSame([], $s->functions);
        self::assertSame([], $s->procedures);
        self::assertSame([], $s->domains);
        self::assertSame([], $s->extensions);
    }

    public function test_schema_with_domains() : void
    {
        $s = schema('public', domains: [
            schema_domain('email', column_type_custom('text'), checkConstraints: [
                schema_check("VALUE ~* '^.+@.+$'"),
            ]),
        ]);

        self::assertCount(1, $s->domains);
        self::assertCount(1, $s->domains[0]->checkConstraints);
    }

    public function test_schema_with_extensions() : void
    {
        $s = schema('public', extensions: [
            schema_extension('uuid-ossp', '1.1'),
        ]);

        self::assertCount(1, $s->extensions);
        self::assertSame('uuid-ossp', $s->extensions[0]->name);
        self::assertSame('1.1', $s->extensions[0]->version);
    }

    public function test_schema_with_functions() : void
    {
        $s = schema('public', functions: [
            schema_function('add', 'integer', argumentTypes: ['integer', 'integer'], isStrict: true, volatility: FunctionVolatility::IMMUTABLE),
        ]);

        self::assertCount(1, $s->functions);
        self::assertTrue($s->functions[0]->isStrict);
        self::assertSame(FunctionVolatility::IMMUTABLE, $s->functions[0]->volatility);
    }

    public function test_schema_with_materialized_views() : void
    {
        $s = schema('public', materializedViews: [
            schema_materialized_view(
                'stats',
                select(agg_count())->from(table('users'))->toSql(),
                indexes: [schema_index('idx_stats', ['count'])],
            ),
        ]);

        self::assertCount(1, $s->materializedViews);
        self::assertCount(1, $s->materializedViews[0]->indexes);
    }

    public function test_schema_with_procedures() : void
    {
        $s = schema('public', procedures: [
            schema_procedure('cleanup', argumentTypes: ['integer'], language: 'plpgsql'),
        ]);

        self::assertCount(1, $s->procedures);
        self::assertSame('plpgsql', $s->procedures[0]->language);
    }

    public function test_schema_with_sequences() : void
    {
        $s = schema('public', sequences: [
            schema_sequence('users_id_seq'),
        ]);

        self::assertTrue($s->hasSequence('users_id_seq'));
        self::assertFalse($s->hasSequence('missing'));
        self::assertSame('users_id_seq', $s->sequence('users_id_seq')->name);
    }

    public function test_schema_with_views() : void
    {
        $s = schema('public', views: [
            schema_view('active_users', select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql(), isUpdatable: true),
        ]);

        self::assertCount(1, $s->views);
        self::assertTrue($s->views[0]->isUpdatable);
    }

    public function test_sequence_not_found_throws() : void
    {
        $this->expectException(SchemaException::class);
        schema('public')->sequence('missing');
    }

    public function test_table_lookup() : void
    {
        $s = schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ]);

        self::assertSame('users', $s->table('users')->name);
    }

    public function test_table_names() : void
    {
        $s = schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
            schema_table('posts', [schema_column_integer('id', nullable: false)]),
        ]);

        self::assertSame(['users', 'posts'], $s->tableNames());
    }

    public function test_table_not_found_throws() : void
    {
        $this->expectException(TableNotFoundException::class);
        schema('public')->table('missing');
    }

    public function test_table_with_all_constraint_types() : void
    {
        $table = schema_table(
            'orders',
            [
                schema_column_serial('id'),
                schema_column_integer('user_id', nullable: false),
                schema_column_numeric('amount', 10, 2, nullable: false),
                schema_column_varchar('status', 50, default: "'pending'"),
            ],
            primaryKey: schema_primary_key(['id'], 'orders_pkey'),
            foreignKeys: [
                schema_foreign_key(
                    ['user_id'],
                    'users',
                    ['id'],
                    name: 'fk_orders_user',
                    onDelete: ReferentialAction::CASCADE,
                    deferrable: true,
                    initiallyDeferred: true,
                ),
            ],
            uniqueConstraints: [schema_unique(['user_id', 'status'], 'uq_user_status', nullsNotDistinct: true)],
            checkConstraints: [schema_check('amount > 0', 'chk_positive_amount', noInherit: true)],
            excludeConstraints: [schema_exclude('USING gist (tsrange WITH &&)')],
        );

        self::assertNotNull($table->primaryKey);
        self::assertSame('orders_pkey', $table->primaryKey->name);
        self::assertCount(1, $table->foreignKeys);
        self::assertTrue($table->foreignKeys[0]->deferrable);
        self::assertTrue($table->foreignKeys[0]->initiallyDeferred);
        self::assertCount(1, $table->uniqueConstraints);
        self::assertTrue($table->uniqueConstraints[0]->nullsNotDistinct);
        self::assertCount(1, $table->checkConstraints);
        self::assertTrue($table->checkConstraints[0]->noInherit);
        self::assertCount(1, $table->excludeConstraints);
    }

    public function test_table_with_multiple_triggers_and_events() : void
    {
        $table = schema_table(
            'orders',
            [schema_column_serial('id')],
            triggers: [
                schema_trigger('trg_before_insert', 'orders', TriggerTiming::BEFORE, [TriggerEvent::INSERT], 'validate_fn', forEachRow: true),
                schema_trigger('trg_after_changes', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT, TriggerEvent::UPDATE, TriggerEvent::DELETE], 'audit_fn', whenCondition: 'NEW.amount > 100'),
                schema_trigger('trg_truncate', 'orders', TriggerTiming::AFTER, [TriggerEvent::TRUNCATE], 'cleanup_fn'),
            ],
        );

        self::assertCount(3, $table->triggers);
        self::assertSame(TriggerTiming::BEFORE, $table->triggers[0]->timing);
        self::assertCount(3, $table->triggers[1]->events);
        self::assertSame('NEW.amount > 100', $table->triggers[1]->whenCondition);
        self::assertFalse($table->triggers[2]->forEachRow);
    }

    public function test_to_sql_includes_all_object_types() : void
    {
        $sqls = schema(
            'public',
            tables: [
                schema_table('users', [schema_column_integer('id', nullable: false)]),
            ],
            sequences: [
                schema_sequence('users_id_seq'),
            ],
            views: [
                schema_view('active_users', select(star())->from(table('users'))->toSql()),
            ],
            materializedViews: [
                schema_materialized_view(
                    'user_counts',
                    select(agg_count())->from(table('users'))->toSql(),
                ),
            ],
            functions: [
                schema_function(
                    'get_one',
                    'integer',
                    language: 'sql',
                    definition: 'SELECT 1',
                ),
            ],
            procedures: [
                schema_procedure(
                    'do_cleanup',
                    language: 'plpgsql',
                    definition: 'BEGIN END;',
                ),
            ],
            domains: [
                schema_domain('positive_int', column_type_integer(), nullable: false),
            ],
            extensions: [
                schema_extension('pgcrypto'),
            ],
        )->toSql();

        self::assertSame('CREATE EXTENSION pgcrypto', $sqls[0]->toSql());
        self::assertSame('CREATE DOMAIN positive_int AS int NOT NULL', $sqls[1]->toSql());
        self::assertSame('CREATE SEQUENCE users_id_seq AS bigint START 1 INCREMENT 1 MINVALUE 1 CACHE 1 NO MAXVALUE', $sqls[2]->toSql());
        self::assertSame('CREATE OR REPLACE FUNCTION get_one() RETURNS int LANGUAGE sql AS $$SELECT 1$$', $sqls[3]->toSql());
        self::assertSame('CREATE OR REPLACE PROCEDURE do_cleanup() LANGUAGE plpgsql AS $$BEGIN END;$$', $sqls[4]->toSql());
        self::assertSame('CREATE TABLE public.users (id int NOT NULL)', $sqls[5]->toSql());
        self::assertSame('CREATE VIEW active_users AS SELECT * FROM users', $sqls[6]->toSql());
        self::assertSame('CREATE MATERIALIZED VIEW user_counts AS SELECT count(*) FROM users', $sqls[7]->toSql());
        self::assertCount(8, $sqls);
    }

    public function test_to_sql_includes_materialized_view_with_indexes() : void
    {
        $sqls = schema(
            'public',
            materializedViews: [
                schema_materialized_view(
                    'user_counts',
                    select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
                    indexes: [schema_index('idx_uc_id', ['id'], unique: true)],
                ),
            ],
        )->toSql();

        self::assertSame('CREATE MATERIALIZED VIEW user_counts AS SELECT id, count(*) FROM users GROUP BY id', $sqls[0]->toSql());
        self::assertSame('CREATE UNIQUE INDEX idx_uc_id ON user_counts (id)', $sqls[1]->toSql());
        self::assertCount(2, $sqls);
    }

    public function test_to_sql_mixes_functions_with_and_without_definitions() : void
    {
        $sqls = schema(
            'public',
            functions: [
                schema_function('builtin_fn', 'integer'),
                schema_function('my_fn', 'integer', language: 'sql', definition: 'SELECT 42'),
                schema_function('another_builtin', 'text'),
            ],
        )->toSql();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE OR REPLACE FUNCTION my_fn() RETURNS int LANGUAGE sql AS $$SELECT 42$$', $sqls[0]->toSql());
    }

    public function test_to_sql_skips_function_with_null_definition() : void
    {
        $sqls = schema(
            'public',
            functions: [
                schema_function('builtin_fn', 'integer'),
            ],
        )->toSql();

        self::assertSame([], $sqls);
    }

    public function test_to_sql_skips_procedure_with_null_definition() : void
    {
        $sqls = schema(
            'public',
            procedures: [
                schema_procedure('builtin_proc'),
            ],
        )->toSql();

        self::assertSame([], $sqls);
    }
}
