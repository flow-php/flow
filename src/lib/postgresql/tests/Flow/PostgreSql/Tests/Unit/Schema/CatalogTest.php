<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\{schema, schema_column_integer, schema_column_text, schema_column_varchar, schema_table};

use Flow\PostgreSql\QueryBuilder\Schema\{ColumnType, ReferentialAction};
use Flow\PostgreSql\Schema\{Catalog, Column, Domain, Extension, Func, FunctionVolatility, IdentityGeneration, Index, IndexMethod, MaterializedView, PartitionStrategy, Procedure, Schema, Sequence, Table, Trigger, TriggerEvent, TriggerTiming, View};
use Flow\PostgreSql\Schema\Constraint\{CheckConstraint, ExcludeConstraint, ForeignKey, PrimaryKey, UniqueConstraint};
use Flow\PostgreSql\Schema\Exception\SchemaException;
use PHPUnit\Framework\TestCase;

final class CatalogTest extends TestCase
{
    public function test_all_returns_all_schemas() : void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
        ]);

        self::assertCount(2, $catalog->all());
        self::assertSame('public', $catalog->all()[0]->name);
        self::assertSame('audit', $catalog->all()[1]->name);
    }

    public function test_empty_catalog() : void
    {
        $catalog = new Catalog([]);

        self::assertSame([], $catalog->all());
        self::assertSame([], $catalog->names());
        self::assertFalse($catalog->has('anything'));
    }

    public function test_empty_catalog_normalize_and_from_array() : void
    {
        $catalog = new Catalog([]);

        $normalized = $catalog->normalize();

        self::assertSame(['schemas' => []], $normalized);
        self::assertSame([], Catalog::fromArray($normalized)->all());
    }

    public function test_from_array_catalog_with_empty_schema() : void
    {
        $catalog = Catalog::fromArray([
            'schemas' => [
                ['name' => 'public'],
                ['name' => 'audit'],
            ],
        ]);

        self::assertSame(['public', 'audit'], $catalog->names());
        self::assertSame([], $catalog->get('public')->tables);
    }

    public function test_from_array_column_with_generated_expression() : void
    {
        $column = Column::fromArray([
            'name' => 'full_name',
            'type' => ['name' => 'text', 'schema' => 'pg_catalog'],
            'nullable' => true,
            'is_generated' => true,
            'generation_expression' => "first_name || ' ' || last_name",
        ]);

        self::assertTrue($column->isGenerated);
        self::assertSame("(first_name || ' ') || last_name", $column->generationExpression);
    }

    public function test_from_array_column_with_identity_always() : void
    {
        $column = Column::fromArray([
            'name' => 'id',
            'type' => ['name' => 'int8', 'schema' => 'pg_catalog'],
            'nullable' => false,
            'is_identity' => true,
            'identity_generation' => 'a',
        ]);

        self::assertSame('id', $column->name);
        self::assertTrue($column->isIdentity);
        self::assertSame(IdentityGeneration::ALWAYS, $column->identityGeneration);
    }

    public function test_from_array_column_with_identity_by_default() : void
    {
        $column = Column::fromArray([
            'name' => 'id',
            'type' => ['name' => 'int8', 'schema' => 'pg_catalog'],
            'nullable' => false,
            'is_identity' => true,
            'identity_generation' => 'd',
        ]);

        self::assertSame(IdentityGeneration::BY_DEFAULT, $column->identityGeneration);
    }

    public function test_from_array_table_with_defaults_only() : void
    {
        $table = Table::fromArray([
            'name' => 'users',
            'columns' => [
                ['name' => 'id', 'type' => ['name' => 'int4', 'schema' => 'pg_catalog'], 'nullable' => false],
            ],
        ]);

        self::assertSame('public', $table->schema);
        self::assertSame('users', $table->name);
        self::assertCount(1, $table->columns);
        self::assertNull($table->primaryKey);
        self::assertSame([], $table->indexes);
        self::assertSame([], $table->foreignKeys);
        self::assertFalse($table->unlogged);
        self::assertNull($table->partitionStrategy);
        self::assertNull($table->tablespace);
    }

    public function test_get_returns_schema_by_name() : void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
        ]);

        self::assertSame('audit', $catalog->get('audit')->name);
    }

    public function test_get_throws_when_schema_not_found() : void
    {
        $catalog = new Catalog([schema('public')]);

        $this->expectException(SchemaException::class);
        $catalog->get('missing');
    }

    public function test_has_returns_false_for_missing_schema() : void
    {
        $catalog = new Catalog([schema('public')]);

        self::assertFalse($catalog->has('missing'));
    }

    public function test_has_returns_true_for_existing_schema() : void
    {
        $catalog = new Catalog([schema('public')]);

        self::assertTrue($catalog->has('public'));
    }

    public function test_merge_catalog_with_empty_catalog() : void
    {
        $catalog = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);

        $result = $catalog->merge(new Catalog([]));

        self::assertSame(['public'], $result->names());
        self::assertTrue($result->get('public')->hasTable('users'));
    }

    public function test_merge_catalogs_with_different_schemas() : void
    {
        $first = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);
        $second = new Catalog([schema('audit', tables: [
            schema_table('logs', [schema_column_integer('id', nullable: false)]),
        ])]);

        $result = $first->merge($second);

        self::assertSame(['public', 'audit'], $result->names());
        self::assertTrue($result->get('public')->hasTable('users'));
        self::assertTrue($result->get('audit')->hasTable('logs'));
    }

    public function test_merge_catalogs_with_mixed_schemas_and_overlapping_tables() : void
    {
        $first = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', nullable: false)]),
                schema_table('posts', [schema_column_integer('id', nullable: false)]),
            ]),
            schema('audit'),
        ]);
        $second = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_text('name')]),
            ]),
            schema('reporting', tables: [
                schema_table('reports', [schema_column_integer('id', nullable: false)]),
            ]),
        ]);

        $result = $first->merge($second);

        self::assertSame(['public', 'audit', 'reporting'], $result->names());
        self::assertTrue($result->get('public')->hasTable('users'));
        self::assertTrue($result->get('public')->hasTable('posts'));
        self::assertCount(1, $result->get('public')->table('users')->columns);
        self::assertSame('name', $result->get('public')->table('users')->columns[0]->name);
        self::assertTrue($result->has('reporting'));
    }

    public function test_merge_catalogs_with_same_schema_merges_tables() : void
    {
        $first = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);
        $second = new Catalog([schema('public', tables: [
            schema_table('posts', [schema_column_integer('id', nullable: false)]),
        ])]);

        $result = $first->merge($second);

        self::assertSame(['public'], $result->names());
        self::assertTrue($result->get('public')->hasTable('users'));
        self::assertTrue($result->get('public')->hasTable('posts'));
    }

    public function test_merge_catalogs_with_same_table_later_overrides() : void
    {
        $first = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);
        $second = new Catalog([schema('public', tables: [
            schema_table('users', [
                schema_column_integer('id', nullable: false),
                schema_column_varchar('email', 255, nullable: false),
            ]),
        ])]);

        $result = $first->merge($second);

        self::assertCount(2, $result->get('public')->table('users')->columns);
    }

    public function test_merge_empty_catalog_with_catalog() : void
    {
        $catalog = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);

        $result = (new Catalog([]))->merge($catalog);

        self::assertSame(['public'], $result->names());
        self::assertTrue($result->get('public')->hasTable('users'));
    }

    public function test_merge_two_empty_catalogs() : void
    {
        $result = (new Catalog([]))->merge(new Catalog([]));

        self::assertSame([], $result->all());
    }

    public function test_names_returns_all_schema_names() : void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
            schema('staging'),
        ]);

        self::assertSame(['public', 'audit', 'staging'], $catalog->names());
    }

    public function test_normalize_and_from_array_with_all_column_types() : void
    {
        $columns = [
            new Column('col_varchar', ColumnType::varchar(255), true),
            new Column('col_numeric', ColumnType::numeric(10, 2), false),
            new Column('col_text_arr', ColumnType::array(ColumnType::text()), true),
            new Column('col_custom', ColumnType::custom('my_enum', 'my_schema'), true),
            new Column('col_char', ColumnType::char(10), true),
            new Column('col_timestamp', ColumnType::timestamp(6), true),
            new Column('col_timestamptz', ColumnType::timestamptz(3), true),
            new Column('col_time', ColumnType::time(0), true),
        ];

        $table = new Table('public', 'type_test', $columns);
        $catalog = new Catalog([new Schema('public', [$table])]);

        $restored = Catalog::fromArray($catalog->normalize());

        $restoredTable = $restored->get('public')->table('type_test');

        foreach ($columns as $i => $originalColumn) {
            self::assertTrue(
                $originalColumn->type->isEqual($restoredTable->columns[$i]->type),
                \sprintf('Column type mismatch for %s', $originalColumn->name),
            );
        }
    }

    public function test_normalize_and_from_array_with_all_index_methods() : void
    {
        $indexes = [];

        foreach (IndexMethod::cases() as $method) {
            $indexes[] = new Index('idx_' . $method->value, ['col1'], method: $method);
        }

        $table = new Table(
            'public',
            'index_test',
            [new Column('col1', ColumnType::text(), true)],
            indexes: $indexes,
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredIndexes = $restored->get('public')->table('index_test')->indexes;

        foreach (IndexMethod::cases() as $i => $method) {
            self::assertSame($method, $restoredIndexes[$i]->method);
        }
    }

    public function test_normalize_and_from_array_with_all_referential_actions() : void
    {
        $foreignKeys = [];

        foreach (ReferentialAction::cases() as $action) {
            $foreignKeys[] = new ForeignKey(
                'fk_' . $action->value,
                ['col1'],
                'public',
                'ref_table',
                ['id'],
                onUpdate: $action,
                onDelete: $action,
            );
        }

        $table = new Table(
            'public',
            'fk_test',
            [new Column('col1', ColumnType::integer(), false)],
            foreignKeys: $foreignKeys,
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredFks = $restored->get('public')->table('fk_test')->foreignKeys;

        foreach (ReferentialAction::cases() as $i => $action) {
            self::assertSame($action, $restoredFks[$i]->onUpdate);
            self::assertSame($action, $restoredFks[$i]->onDelete);
        }
    }

    public function test_normalize_and_from_array_with_all_trigger_events() : void
    {
        $trigger = new Trigger(
            'trigger_all_events',
            'test_table',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT, TriggerEvent::UPDATE, TriggerEvent::DELETE, TriggerEvent::TRUNCATE],
            'my_function',
            forEachRow: true,
            whenCondition: 'NEW.status IS DISTINCT FROM OLD.status',
        );

        $table = new Table(
            'public',
            'test_table',
            [new Column('id', ColumnType::integer(), false)],
            triggers: [$trigger],
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredTrigger = $restored->get('public')->table('test_table')->triggers[0];

        self::assertSame(
            [TriggerEvent::INSERT, TriggerEvent::UPDATE, TriggerEvent::DELETE, TriggerEvent::TRUNCATE],
            $restoredTrigger->events,
        );
        self::assertTrue($restoredTrigger->forEachRow);
        self::assertSame('NEW.status IS DISTINCT FROM OLD.status', $restoredTrigger->whenCondition);
    }

    public function test_normalize_and_from_array_with_all_trigger_timings_and_events() : void
    {
        $triggers = [];

        foreach (TriggerTiming::cases() as $timing) {
            $triggers[] = new Trigger(
                'trigger_' . $timing->value,
                'test_table',
                $timing,
                [TriggerEvent::INSERT, TriggerEvent::UPDATE],
                'my_function',
            );
        }

        $table = new Table(
            'public',
            'test_table',
            [new Column('id', ColumnType::integer(), false)],
            triggers: $triggers,
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredTriggers = $restored->get('public')->table('test_table')->triggers;

        foreach (TriggerTiming::cases() as $i => $timing) {
            self::assertSame($timing, $restoredTriggers[$i]->timing);
            self::assertSame([TriggerEvent::INSERT, TriggerEvent::UPDATE], $restoredTriggers[$i]->events);
        }
    }

    public function test_normalize_and_from_array_with_function_volatility() : void
    {
        $functions = [];

        foreach (FunctionVolatility::cases() as $volatility) {
            $functions[] = new Func(
                'func_' . $volatility->value,
                'text',
                ['text'],
                'sql',
                'SELECT $1',
                volatility: $volatility,
            );
        }

        $functions[] = new Func('func_no_volatility', 'void', definition: 'SELECT 1');

        $catalog = new Catalog([new Schema('public', functions: $functions)]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredFunctions = $restored->get('public')->functions;

        foreach (FunctionVolatility::cases() as $i => $volatility) {
            self::assertSame($volatility, $restoredFunctions[$i]->volatility);
        }

        self::assertNull($restoredFunctions[\count(FunctionVolatility::cases())]->volatility);
    }

    public function test_normalize_and_from_array_with_partition_strategies() : void
    {
        $tables = [];

        foreach (PartitionStrategy::cases() as $strategy) {
            $tables[] = new Table(
                'public',
                'partitioned_' . $strategy->value,
                [new Column('id', ColumnType::integer(), false)],
                partitionStrategy: $strategy,
                partitionColumns: ['id'],
            );
        }

        $catalog = new Catalog([new Schema('public', $tables)]);
        $restored = Catalog::fromArray($catalog->normalize());

        foreach (PartitionStrategy::cases() as $i => $strategy) {
            self::assertSame($strategy, $restored->get('public')->tables[$i]->partitionStrategy);
            self::assertSame(['id'], $restored->get('public')->tables[$i]->partitionColumns);
        }
    }

    public function test_normalize_and_from_array_with_schema_containing_all_object_types() : void
    {
        $catalog = new Catalog([
            new Schema(
                'public',
                tables: [
                    new Table(
                        'public',
                        'users',
                        [
                            new Column('id', ColumnType::bigint(), false, isIdentity: true, identityGeneration: IdentityGeneration::ALWAYS),
                            new Column('email', ColumnType::varchar(255), false),
                            new Column('name', ColumnType::text(), true),
                        ],
                        primaryKey: new PrimaryKey(['id'], 'users_pkey'),
                        indexes: [
                            new Index('idx_users_email', ['email'], unique: true),
                        ],
                        uniqueConstraints: [
                            new UniqueConstraint(['email'], 'uq_email'),
                        ],
                    ),
                ],
                sequences: [
                    new Sequence('users_id_seq', 'bigint', startValue: 1, incrementBy: 1),
                ],
                views: [
                    new View('active_users', 'SELECT * FROM users WHERE active = true'),
                ],
                materializedViews: [
                    new MaterializedView(
                        'user_stats',
                        'SELECT count(*) FROM users',
                        [new Index('idx_user_stats', ['count'])],
                    ),
                ],
                functions: [
                    new Func('get_user', 'text', ['integer'], 'sql', 'SELECT name FROM users WHERE id = $1', isStrict: true, volatility: FunctionVolatility::STABLE),
                ],
                procedures: [
                    new Procedure('cleanup', [], 'sql', 'DELETE FROM users WHERE active = false'),
                ],
                domains: [
                    new Domain(
                        'email_address',
                        ColumnType::varchar(255),
                        nullable: false,
                        checkConstraints: [new CheckConstraint("VALUE ~ '^.+@.+$'", 'email_format_check')],
                    ),
                ],
                extensions: [
                    new Extension('uuid-ossp', '1.1'),
                ],
            ),
        ]);

        $normalized = $catalog->normalize();
        $restored = Catalog::fromArray($normalized);

        self::assertSame(['public'], $restored->names());

        $restoredSchema = $restored->get('public');

        self::assertCount(1, $restoredSchema->tables);
        self::assertCount(1, $restoredSchema->sequences);
        self::assertCount(1, $restoredSchema->views);
        self::assertCount(1, $restoredSchema->materializedViews);
        self::assertCount(1, $restoredSchema->functions);
        self::assertCount(1, $restoredSchema->procedures);
        self::assertCount(1, $restoredSchema->domains);
        self::assertCount(1, $restoredSchema->extensions);

        $restoredTable = $restoredSchema->table('users');
        self::assertCount(3, $restoredTable->columns);
        self::assertSame('users_pkey', $restoredTable->primaryKey->name);
        self::assertCount(1, $restoredTable->indexes);
        self::assertCount(1, $restoredTable->uniqueConstraints);

        self::assertSame('users_id_seq', $restoredSchema->sequences[0]->name);
        self::assertSame('active_users', $restoredSchema->views[0]->name);
        self::assertSame('user_stats', $restoredSchema->materializedViews[0]->name);
        self::assertSame('get_user', $restoredSchema->functions[0]->name);
        self::assertSame('cleanup', $restoredSchema->procedures[0]->name);
        self::assertSame('email_address', $restoredSchema->domains[0]->name);
        self::assertSame('uuid-ossp', $restoredSchema->extensions[0]->name);
        self::assertSame('1.1', $restoredSchema->extensions[0]->version);
    }

    public function test_normalize_and_from_array_with_table_all_constraints() : void
    {
        $table = new Table(
            'public',
            'orders',
            [
                new Column('id', ColumnType::integer(), false),
                new Column('user_id', ColumnType::integer(), false),
                new Column('amount', ColumnType::numeric(10, 2), false),
                new Column('status', ColumnType::text(), false, default: "'pending'"),
            ],
            primaryKey: new PrimaryKey(['id'], 'orders_pkey'),
            indexes: [
                new Index('idx_orders_user', ['user_id'], method: IndexMethod::BTREE),
                new Index('idx_orders_amount', ['amount'], method: IndexMethod::BRIN),
            ],
            foreignKeys: [
                new ForeignKey(
                    'fk_orders_user',
                    ['user_id'],
                    'public',
                    'users',
                    ['id'],
                    onUpdate: ReferentialAction::CASCADE,
                    onDelete: ReferentialAction::SET_NULL,
                    deferrable: true,
                    initiallyDeferred: true,
                ),
            ],
            uniqueConstraints: [
                new UniqueConstraint(['id', 'user_id'], 'uq_order_user', nullsNotDistinct: true),
            ],
            checkConstraints: [
                new CheckConstraint('amount > 0', 'chk_positive_amount', noInherit: true),
            ],
            excludeConstraints: [
                new ExcludeConstraint('USING gist (daterange(start_date, end_date) WITH &&)', 'excl_date_range'),
            ],
            triggers: [
                new Trigger(
                    'trg_orders_audit',
                    'orders',
                    TriggerTiming::AFTER,
                    [TriggerEvent::INSERT, TriggerEvent::UPDATE, TriggerEvent::DELETE],
                    'audit_function',
                    forEachRow: true,
                    whenCondition: 'NEW.amount > 1000',
                ),
            ],
            unlogged: true,
            tablespace: 'fast_storage',
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $normalized = $catalog->normalize();
        $restored = Catalog::fromArray($normalized);

        $restoredTable = $restored->get('public')->table('orders');

        self::assertSame('orders_pkey', $restoredTable->primaryKey->name);
        self::assertSame(['id'], $restoredTable->primaryKey->columns);

        self::assertCount(2, $restoredTable->indexes);
        self::assertSame(IndexMethod::BRIN, $restoredTable->indexes[1]->method);

        self::assertCount(1, $restoredTable->foreignKeys);
        self::assertSame(ReferentialAction::CASCADE, $restoredTable->foreignKeys[0]->onUpdate);
        self::assertSame(ReferentialAction::SET_NULL, $restoredTable->foreignKeys[0]->onDelete);
        self::assertTrue($restoredTable->foreignKeys[0]->deferrable);
        self::assertTrue($restoredTable->foreignKeys[0]->initiallyDeferred);

        self::assertCount(1, $restoredTable->uniqueConstraints);
        self::assertTrue($restoredTable->uniqueConstraints[0]->nullsNotDistinct);

        self::assertCount(1, $restoredTable->checkConstraints);
        self::assertTrue($restoredTable->checkConstraints[0]->noInherit);
        self::assertSame('amount > 0', $restoredTable->checkConstraints[0]->expression);

        self::assertCount(1, $restoredTable->excludeConstraints);
        self::assertSame('excl_date_range', $restoredTable->excludeConstraints[0]->name);

        self::assertCount(1, $restoredTable->triggers);
        self::assertSame(TriggerTiming::AFTER, $restoredTable->triggers[0]->timing);
        self::assertTrue($restoredTable->triggers[0]->forEachRow);
        self::assertSame('NEW.amount > 1000', $restoredTable->triggers[0]->whenCondition);

        self::assertTrue($restoredTable->unlogged);
        self::assertSame('fast_storage', $restoredTable->tablespace);
    }

    public function test_normalize_and_from_array_with_table_inherits() : void
    {
        $table = new Table(
            'public',
            'child_table',
            [new Column('id', ColumnType::integer(), false)],
            inherits: ['parent_table_1', 'parent_table_2'],
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        self::assertSame(['parent_table_1', 'parent_table_2'], $restored->get('public')->table('child_table')->inherits);
    }

    public function test_normalize_produces_expected_structure() : void
    {
        $catalog = new Catalog([
            new Schema('public', [
                new Table(
                    'public',
                    'simple',
                    [new Column('id', ColumnType::integer(), false)],
                ),
            ]),
        ]);

        $normalized = $catalog->normalize();

        self::assertArrayHasKey('schemas', $normalized);
        self::assertCount(1, $normalized['schemas']);
        self::assertSame('public', $normalized['schemas'][0]['name']);
        self::assertArrayHasKey('tables', $normalized['schemas'][0]);
        self::assertSame('simple', $normalized['schemas'][0]['tables'][0]['name']);
        self::assertSame('public', $normalized['schemas'][0]['tables'][0]['schema']);
        self::assertArrayHasKey('columns', $normalized['schemas'][0]['tables'][0]);
        self::assertSame('id', $normalized['schemas'][0]['tables'][0]['columns'][0]['name']);
        self::assertSame('int4', $normalized['schemas'][0]['tables'][0]['columns'][0]['type']['name']);
    }

    public function test_round_trip_preserves_column_default_value() : void
    {
        $table = new Table(
            'public',
            'defaults',
            [
                new Column('status', ColumnType::text(), false, default: "'active'"),
                new Column('count', ColumnType::integer(), false, default: '0'),
                new Column('no_default', ColumnType::text(), true),
            ],
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredTable = $restored->get('public')->table('defaults');
        self::assertSame("'active'", $restoredTable->column('status')->default);
        self::assertSame('0', $restoredTable->column('count')->default);
        self::assertNull($restoredTable->column('no_default')->default);
    }

    public function test_round_trip_preserves_domain_with_check_constraints() : void
    {
        $domain = new Domain(
            'positive_int',
            ColumnType::integer(),
            nullable: false,
            default: '0',
            checkConstraints: [
                new CheckConstraint('VALUE > 0', 'chk_positive'),
                new CheckConstraint('VALUE < 1000000', 'chk_max'),
            ],
        );

        $catalog = new Catalog([new Schema('public', domains: [$domain])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredDomain = $restored->get('public')->domains[0];
        self::assertSame('positive_int', $restoredDomain->name);
        self::assertFalse($restoredDomain->nullable);
        self::assertSame('0', $restoredDomain->default);
        self::assertCount(2, $restoredDomain->checkConstraints);
        self::assertSame('value > 0', $restoredDomain->checkConstraints[0]->expression);
        self::assertSame('chk_positive', $restoredDomain->checkConstraints[0]->name);
    }

    public function test_round_trip_preserves_index_predicate() : void
    {
        $index = new Index('idx_partial', ['status'], predicate: "status = 'active'");

        $table = new Table(
            'public',
            'test',
            [new Column('status', ColumnType::text(), true)],
            indexes: [$index],
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        self::assertSame("status = 'active'", $restored->get('public')->table('test')->indexes[0]->predicate);
    }

    public function test_round_trip_preserves_materialized_view_indexes() : void
    {
        $mv = new MaterializedView(
            'stats',
            'SELECT count(*) as cnt FROM users',
            [
                new Index('idx_stats_cnt', ['cnt'], unique: true, method: IndexMethod::BTREE),
            ],
        );

        $catalog = new Catalog([new Schema('public', materializedViews: [$mv])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredMv = $restored->get('public')->materializedViews[0];
        self::assertSame('stats', $restoredMv->name);
        self::assertCount(1, $restoredMv->indexes);
        self::assertTrue($restoredMv->indexes[0]->unique);
        self::assertSame('idx_stats_cnt', $restoredMv->indexes[0]->name);
    }

    public function test_round_trip_preserves_multiple_schemas() : void
    {
        $catalog = new Catalog([
            new Schema('public', [
                new Table('public', 'users', [new Column('id', ColumnType::integer(), false)]),
            ]),
            new Schema('audit', [
                new Table('audit', 'logs', [new Column('id', ColumnType::bigint(), false)]),
            ]),
            new Schema('staging'),
        ]);

        $restored = Catalog::fromArray($catalog->normalize());

        self::assertSame(['public', 'audit', 'staging'], $restored->names());
        self::assertTrue($restored->get('public')->hasTable('users'));
        self::assertTrue($restored->get('audit')->hasTable('logs'));
        self::assertSame([], $restored->get('staging')->tables);
    }

    public function test_round_trip_preserves_nullable_fields_as_null() : void
    {
        $table = new Table(
            'public',
            'nullable_test',
            [
                new Column('id', ColumnType::integer(), false),
            ],
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredTable = $restored->get('public')->table('nullable_test');
        self::assertNull($restoredTable->primaryKey);
        self::assertNull($restoredTable->partitionStrategy);
        self::assertNull($restoredTable->tablespace);
        self::assertNull($restoredTable->columns[0]->default);
        self::assertNull($restoredTable->columns[0]->identityGeneration);
        self::assertNull($restoredTable->columns[0]->generationExpression);
        self::assertNull($restoredTable->columns[0]->ordinalPosition);
    }

    public function test_round_trip_preserves_procedure() : void
    {
        $procedure = new Procedure(
            'cleanup_proc',
            ['integer', 'text'],
            'plpgsql',
            'BEGIN DELETE FROM logs WHERE age > $1; END;',
        );

        $catalog = new Catalog([new Schema('public', procedures: [$procedure])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredProc = $restored->get('public')->procedures[0];
        self::assertSame('cleanup_proc', $restoredProc->name);
        self::assertSame(['integer', 'text'], $restoredProc->argumentTypes);
        self::assertSame('plpgsql', $restoredProc->language);
        self::assertSame('BEGIN DELETE FROM logs WHERE age > $1; END;', $restoredProc->definition);
    }

    public function test_round_trip_preserves_sequence_all_fields() : void
    {
        $sequence = new Sequence(
            'custom_seq',
            'integer',
            startValue: 100,
            minValue: 1,
            maxValue: 999999,
            incrementBy: 5,
            cycle: true,
            cacheValue: 10,
            ownedByTable: 'users',
            ownedByColumn: 'id',
        );

        $catalog = new Catalog([new Schema('public', sequences: [$sequence])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredSeq = $restored->get('public')->sequences[0];
        self::assertSame('custom_seq', $restoredSeq->name);
        self::assertSame('integer', $restoredSeq->dataType);
        self::assertSame(100, $restoredSeq->startValue);
        self::assertSame(1, $restoredSeq->minValue);
        self::assertSame(999999, $restoredSeq->maxValue);
        self::assertSame(5, $restoredSeq->incrementBy);
        self::assertTrue($restoredSeq->cycle);
        self::assertSame(10, $restoredSeq->cacheValue);
        self::assertSame('users', $restoredSeq->ownedByTable);
        self::assertSame('id', $restoredSeq->ownedByColumn);
    }

    public function test_round_trip_preserves_strict_function() : void
    {
        $func = new Func(
            'my_func',
            'boolean',
            ['text', 'integer'],
            'plpgsql',
            'BEGIN RETURN true; END;',
            isStrict: true,
            volatility: FunctionVolatility::IMMUTABLE,
        );

        $catalog = new Catalog([new Schema('public', functions: [$func])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredFunc = $restored->get('public')->functions[0];
        self::assertSame('my_func', $restoredFunc->name);
        self::assertSame('boolean', $restoredFunc->returnType);
        self::assertSame(['text', 'integer'], $restoredFunc->argumentTypes);
        self::assertSame('plpgsql', $restoredFunc->language);
        self::assertTrue($restoredFunc->isStrict);
        self::assertSame(FunctionVolatility::IMMUTABLE, $restoredFunc->volatility);
    }

    public function test_round_trip_preserves_view() : void
    {
        $view = new View('my_view', 'SELECT id, name FROM users', isUpdatable: true);

        $catalog = new Catalog([new Schema('public', views: [$view])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredView = $restored->get('public')->views[0];
        self::assertSame('my_view', $restoredView->name);
        self::assertSame('SELECT id, name FROM users', $restoredView->definition);
        self::assertTrue($restoredView->isUpdatable);
    }
}
