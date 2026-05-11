<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Constraint\ExcludeConstraint;
use Flow\PostgreSql\Schema\Constraint\ForeignKey;
use Flow\PostgreSql\Schema\Constraint\PrimaryKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\Schema\Domain;
use Flow\PostgreSql\Schema\Exception\SchemaException;
use Flow\PostgreSql\Schema\Extension;
use Flow\PostgreSql\Schema\Func;
use Flow\PostgreSql\Schema\FunctionVolatility;
use Flow\PostgreSql\Schema\IdentityGeneration;
use Flow\PostgreSql\Schema\Index;
use Flow\PostgreSql\Schema\IndexMethod;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\PartitionStrategy;
use Flow\PostgreSql\Schema\Procedure;
use Flow\PostgreSql\Schema\Schema;
use Flow\PostgreSql\Schema\Sequence;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\Trigger;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use Flow\PostgreSql\Schema\View;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_column_varchar;
use function Flow\PostgreSql\DSL\schema_table;

final class CatalogTest extends TestCase
{
    public function test_all_returns_all_schemas(): void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
        ]);

        static::assertCount(2, $catalog->all());
        static::assertSame('public', $catalog->all()[0]->name);
        static::assertSame('audit', $catalog->all()[1]->name);
    }

    public function test_empty_catalog(): void
    {
        $catalog = new Catalog([]);

        static::assertSame([], $catalog->all());
        static::assertSame([], $catalog->names());
        static::assertFalse($catalog->has('anything'));
    }

    public function test_empty_catalog_normalize_and_from_array(): void
    {
        $catalog = new Catalog([]);

        $normalized = $catalog->normalize();

        static::assertSame(['schemas' => []], $normalized);
        static::assertSame([], Catalog::fromArray($normalized)->all());
    }

    public function test_from_array_catalog_with_empty_schema(): void
    {
        $catalog = Catalog::fromArray([
            'schemas' => [
                ['name' => 'public'],
                ['name' => 'audit'],
            ],
        ]);

        static::assertSame(['public', 'audit'], $catalog->names());
        static::assertSame([], $catalog->get('public')->tables);
    }

    public function test_from_array_column_with_generated_expression(): void
    {
        $column = Column::fromArray([
            'name' => 'full_name',
            'type' => ['name' => 'text', 'schema' => 'pg_catalog'],
            'nullable' => true,
            'is_generated' => true,
            'generation_expression' => "first_name || ' ' || last_name",
        ]);

        static::assertTrue($column->isGenerated);
        static::assertSame("(first_name || ' ') || last_name", $column->generationExpression);
    }

    public function test_from_array_column_with_identity_always(): void
    {
        $column = Column::fromArray([
            'name' => 'id',
            'type' => ['name' => 'int8', 'schema' => 'pg_catalog'],
            'nullable' => false,
            'is_identity' => true,
            'identity_generation' => 'a',
        ]);

        static::assertSame('id', $column->name);
        static::assertTrue($column->isIdentity);
        static::assertSame(IdentityGeneration::ALWAYS, $column->identityGeneration);
    }

    public function test_from_array_column_with_identity_by_default(): void
    {
        $column = Column::fromArray([
            'name' => 'id',
            'type' => ['name' => 'int8', 'schema' => 'pg_catalog'],
            'nullable' => false,
            'is_identity' => true,
            'identity_generation' => 'd',
        ]);

        static::assertSame(IdentityGeneration::BY_DEFAULT, $column->identityGeneration);
    }

    public function test_from_array_table_with_defaults_only(): void
    {
        $table = Table::fromArray([
            'name' => 'users',
            'columns' => [
                ['name' => 'id', 'type' => ['name' => 'int4', 'schema' => 'pg_catalog'], 'nullable' => false],
            ],
        ]);

        static::assertSame('public', $table->schema);
        static::assertSame('users', $table->name);
        static::assertCount(1, $table->columns);
        static::assertNull($table->primaryKey);
        static::assertSame([], $table->indexes);
        static::assertSame([], $table->foreignKeys);
        static::assertFalse($table->unlogged);
        static::assertNull($table->partitionStrategy);
        static::assertNull($table->tablespace);
    }

    public function test_get_returns_schema_by_name(): void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
        ]);

        static::assertSame('audit', $catalog->get('audit')->name);
    }

    public function test_get_throws_when_schema_not_found(): void
    {
        $catalog = new Catalog([schema('public')]);

        $this->expectException(SchemaException::class);
        $catalog->get('missing');
    }

    public function test_has_returns_false_for_missing_schema(): void
    {
        $catalog = new Catalog([schema('public')]);

        static::assertFalse($catalog->has('missing'));
    }

    public function test_has_returns_true_for_existing_schema(): void
    {
        $catalog = new Catalog([schema('public')]);

        static::assertTrue($catalog->has('public'));
    }

    public function test_merge_catalog_with_empty_catalog(): void
    {
        $catalog = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);

        $result = $catalog->merge(new Catalog([]));

        static::assertSame(['public'], $result->names());
        static::assertTrue($result->get('public')->hasTable('users'));
    }

    public function test_merge_catalogs_with_different_schemas(): void
    {
        $first = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);
        $second = new Catalog([schema('audit', tables: [
            schema_table('logs', [schema_column_integer('id', nullable: false)]),
        ])]);

        $result = $first->merge($second);

        static::assertSame(['public', 'audit'], $result->names());
        static::assertTrue($result->get('public')->hasTable('users'));
        static::assertTrue($result->get('audit')->hasTable('logs'));
    }

    public function test_merge_catalogs_with_mixed_schemas_and_overlapping_tables(): void
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

        static::assertSame(['public', 'audit', 'reporting'], $result->names());
        static::assertTrue($result->get('public')->hasTable('users'));
        static::assertTrue($result->get('public')->hasTable('posts'));
        static::assertCount(1, $result->get('public')->table('users')->columns);
        static::assertSame('name', $result->get('public')->table('users')->columns[0]->name);
        static::assertTrue($result->has('reporting'));
    }

    public function test_merge_catalogs_with_same_schema_merges_tables(): void
    {
        $first = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);
        $second = new Catalog([schema('public', tables: [
            schema_table('posts', [schema_column_integer('id', nullable: false)]),
        ])]);

        $result = $first->merge($second);

        static::assertSame(['public'], $result->names());
        static::assertTrue($result->get('public')->hasTable('users'));
        static::assertTrue($result->get('public')->hasTable('posts'));
    }

    public function test_merge_catalogs_with_same_table_later_overrides(): void
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

        static::assertCount(2, $result->get('public')->table('users')->columns);
    }

    public function test_merge_empty_catalog_with_catalog(): void
    {
        $catalog = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);

        $result = (new Catalog([]))->merge($catalog);

        static::assertSame(['public'], $result->names());
        static::assertTrue($result->get('public')->hasTable('users'));
    }

    public function test_merge_two_empty_catalogs(): void
    {
        $result = (new Catalog([]))->merge(new Catalog([]));

        static::assertSame([], $result->all());
    }

    public function test_names_returns_all_schema_names(): void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
            schema('staging'),
        ]);

        static::assertSame(['public', 'audit', 'staging'], $catalog->names());
    }

    public function test_normalize_and_from_array_with_all_column_types(): void
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
            static::assertTrue(
                $originalColumn->type->isEqual($restoredTable->columns[$i]->type),
                \sprintf('Column type mismatch for %s', $originalColumn->name),
            );
        }
    }

    public function test_normalize_and_from_array_with_all_index_methods(): void
    {
        $indexes = [];

        foreach (IndexMethod::cases() as $method) {
            $indexes[] = new Index('idx_' . $method->value, ['col1'], method: $method);
        }

        $table = new Table('public', 'index_test', [new Column('col1', ColumnType::text(), true)], indexes: $indexes);

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredIndexes = $restored->get('public')->table('index_test')->indexes;

        foreach (IndexMethod::cases() as $i => $method) {
            static::assertSame($method, $restoredIndexes[$i]->method);
        }
    }

    public function test_normalize_and_from_array_with_all_referential_actions(): void
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
            static::assertSame($action, $restoredFks[$i]->onUpdate);
            static::assertSame($action, $restoredFks[$i]->onDelete);
        }
    }

    public function test_normalize_and_from_array_with_all_trigger_events(): void
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

        static::assertSame(
            [TriggerEvent::INSERT, TriggerEvent::UPDATE, TriggerEvent::DELETE, TriggerEvent::TRUNCATE],
            $restoredTrigger->events,
        );
        static::assertTrue($restoredTrigger->forEachRow);
        static::assertSame('NEW.status IS DISTINCT FROM OLD.status', $restoredTrigger->whenCondition);
    }

    public function test_normalize_and_from_array_with_all_trigger_timings_and_events(): void
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
            static::assertSame($timing, $restoredTriggers[$i]->timing);
            static::assertSame([TriggerEvent::INSERT, TriggerEvent::UPDATE], $restoredTriggers[$i]->events);
        }
    }

    public function test_normalize_and_from_array_with_function_volatility(): void
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
            static::assertSame($volatility, $restoredFunctions[$i]->volatility);
        }

        static::assertNull($restoredFunctions[\count(FunctionVolatility::cases())]->volatility);
    }

    public function test_normalize_and_from_array_with_partition_strategies(): void
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
            static::assertSame($strategy, $restored->get('public')->tables[$i]->partitionStrategy);
            static::assertSame(['id'], $restored->get('public')->tables[$i]->partitionColumns);
        }
    }

    public function test_normalize_and_from_array_with_schema_containing_all_object_types(): void
    {
        $catalog = new Catalog([
            new Schema(
                'public',
                tables: [
                    new Table(
                        'public',
                        'users',
                        [
                            new Column(
                                'id',
                                ColumnType::bigint(),
                                false,
                                isIdentity: true,
                                identityGeneration: IdentityGeneration::ALWAYS,
                            ),
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
                    new MaterializedView('user_stats', 'SELECT count(*) FROM users', [new Index('idx_user_stats', [
                        'count',
                    ])]),
                ],
                functions: [
                    new Func(
                        'get_user',
                        'text',
                        ['integer'],
                        'sql',
                        'SELECT name FROM users WHERE id = $1',
                        isStrict: true,
                        volatility: FunctionVolatility::STABLE,
                    ),
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

        static::assertSame(['public'], $restored->names());

        $restoredSchema = $restored->get('public');

        static::assertCount(1, $restoredSchema->tables);
        static::assertCount(1, $restoredSchema->sequences);
        static::assertCount(1, $restoredSchema->views);
        static::assertCount(1, $restoredSchema->materializedViews);
        static::assertCount(1, $restoredSchema->functions);
        static::assertCount(1, $restoredSchema->procedures);
        static::assertCount(1, $restoredSchema->domains);
        static::assertCount(1, $restoredSchema->extensions);

        $restoredTable = $restoredSchema->table('users');
        static::assertCount(3, $restoredTable->columns);
        static::assertSame('users_pkey', $restoredTable->primaryKey->name);
        static::assertCount(1, $restoredTable->indexes);
        static::assertCount(1, $restoredTable->uniqueConstraints);

        static::assertSame('users_id_seq', $restoredSchema->sequences[0]->name);
        static::assertSame('active_users', $restoredSchema->views[0]->name);
        static::assertSame('user_stats', $restoredSchema->materializedViews[0]->name);
        static::assertSame('get_user', $restoredSchema->functions[0]->name);
        static::assertSame('cleanup', $restoredSchema->procedures[0]->name);
        static::assertSame('email_address', $restoredSchema->domains[0]->name);
        static::assertSame('uuid-ossp', $restoredSchema->extensions[0]->name);
        static::assertSame('1.1', $restoredSchema->extensions[0]->version);
    }

    public function test_normalize_and_from_array_with_table_all_constraints(): void
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

        static::assertSame('orders_pkey', $restoredTable->primaryKey->name);
        static::assertSame(['id'], $restoredTable->primaryKey->columns);

        static::assertCount(2, $restoredTable->indexes);
        static::assertSame(IndexMethod::BRIN, $restoredTable->indexes[1]->method);

        static::assertCount(1, $restoredTable->foreignKeys);
        static::assertSame(ReferentialAction::CASCADE, $restoredTable->foreignKeys[0]->onUpdate);
        static::assertSame(ReferentialAction::SET_NULL, $restoredTable->foreignKeys[0]->onDelete);
        static::assertTrue($restoredTable->foreignKeys[0]->deferrable);
        static::assertTrue($restoredTable->foreignKeys[0]->initiallyDeferred);

        static::assertCount(1, $restoredTable->uniqueConstraints);
        static::assertTrue($restoredTable->uniqueConstraints[0]->nullsNotDistinct);

        static::assertCount(1, $restoredTable->checkConstraints);
        static::assertTrue($restoredTable->checkConstraints[0]->noInherit);
        static::assertSame('amount > 0', $restoredTable->checkConstraints[0]->expression);

        static::assertCount(1, $restoredTable->excludeConstraints);
        static::assertSame('excl_date_range', $restoredTable->excludeConstraints[0]->name);

        static::assertCount(1, $restoredTable->triggers);
        static::assertSame(TriggerTiming::AFTER, $restoredTable->triggers[0]->timing);
        static::assertTrue($restoredTable->triggers[0]->forEachRow);
        static::assertSame('NEW.amount > 1000', $restoredTable->triggers[0]->whenCondition);

        static::assertTrue($restoredTable->unlogged);
        static::assertSame('fast_storage', $restoredTable->tablespace);
    }

    public function test_normalize_and_from_array_with_table_inherits(): void
    {
        $table = new Table(
            'public',
            'child_table',
            [new Column('id', ColumnType::integer(), false)],
            inherits: ['parent_table_1', 'parent_table_2'],
        );

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        static::assertSame(
            ['parent_table_1', 'parent_table_2'],
            $restored->get('public')->table('child_table')->inherits,
        );
    }

    public function test_normalize_produces_expected_structure(): void
    {
        $catalog = new Catalog([
            new Schema('public', [
                new Table('public', 'simple', [new Column('id', ColumnType::integer(), false)]),
            ]),
        ]);

        $normalized = $catalog->normalize();

        static::assertArrayHasKey('schemas', $normalized);
        static::assertCount(1, $normalized['schemas']);
        static::assertSame('public', $normalized['schemas'][0]['name']);
        static::assertArrayHasKey('tables', $normalized['schemas'][0]);
        static::assertSame('simple', $normalized['schemas'][0]['tables'][0]['name']);
        static::assertSame('public', $normalized['schemas'][0]['tables'][0]['schema']);
        static::assertArrayHasKey('columns', $normalized['schemas'][0]['tables'][0]);
        static::assertSame('id', $normalized['schemas'][0]['tables'][0]['columns'][0]['name']);
        static::assertSame('int4', $normalized['schemas'][0]['tables'][0]['columns'][0]['type']['name']);
    }

    public function test_round_trip_preserves_column_default_value(): void
    {
        $table = new Table('public', 'defaults', [
            new Column('status', ColumnType::text(), false, default: "'active'"),
            new Column('count', ColumnType::integer(), false, default: '0'),
            new Column('no_default', ColumnType::text(), true),
        ]);

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredTable = $restored->get('public')->table('defaults');
        static::assertSame("'active'", $restoredTable->column('status')->default);
        static::assertSame('0', $restoredTable->column('count')->default);
        static::assertNull($restoredTable->column('no_default')->default);
    }

    public function test_round_trip_preserves_domain_with_check_constraints(): void
    {
        $domain = new Domain('positive_int', ColumnType::integer(), nullable: false, default: '0', checkConstraints: [
            new CheckConstraint('VALUE > 0', 'chk_positive'),
            new CheckConstraint('VALUE < 1000000', 'chk_max'),
        ]);

        $catalog = new Catalog([new Schema('public', domains: [$domain])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredDomain = $restored->get('public')->domains[0];
        static::assertSame('positive_int', $restoredDomain->name);
        static::assertFalse($restoredDomain->nullable);
        static::assertSame('0', $restoredDomain->default);
        static::assertCount(2, $restoredDomain->checkConstraints);
        static::assertSame('value > 0', $restoredDomain->checkConstraints[0]->expression);
        static::assertSame('chk_positive', $restoredDomain->checkConstraints[0]->name);
    }

    public function test_round_trip_preserves_index_predicate(): void
    {
        $index = new Index('idx_partial', ['status'], predicate: "status = 'active'");

        $table = new Table('public', 'test', [new Column('status', ColumnType::text(), true)], indexes: [$index]);

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        static::assertSame("status = 'active'", $restored->get('public')->table('test')->indexes[0]->predicate);
    }

    public function test_round_trip_preserves_materialized_view_indexes(): void
    {
        $mv = new MaterializedView('stats', 'SELECT count(*) as cnt FROM users', [
            new Index('idx_stats_cnt', ['cnt'], unique: true, method: IndexMethod::BTREE),
        ]);

        $catalog = new Catalog([new Schema('public', materializedViews: [$mv])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredMv = $restored->get('public')->materializedViews[0];
        static::assertSame('stats', $restoredMv->name);
        static::assertCount(1, $restoredMv->indexes);
        static::assertTrue($restoredMv->indexes[0]->unique);
        static::assertSame('idx_stats_cnt', $restoredMv->indexes[0]->name);
    }

    public function test_round_trip_preserves_multiple_schemas(): void
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

        static::assertSame(['public', 'audit', 'staging'], $restored->names());
        static::assertTrue($restored->get('public')->hasTable('users'));
        static::assertTrue($restored->get('audit')->hasTable('logs'));
        static::assertSame([], $restored->get('staging')->tables);
    }

    public function test_round_trip_preserves_nullable_fields_as_null(): void
    {
        $table = new Table('public', 'nullable_test', [
            new Column('id', ColumnType::integer(), false),
        ]);

        $catalog = new Catalog([new Schema('public', [$table])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredTable = $restored->get('public')->table('nullable_test');
        static::assertNull($restoredTable->primaryKey);
        static::assertNull($restoredTable->partitionStrategy);
        static::assertNull($restoredTable->tablespace);
        static::assertNull($restoredTable->columns[0]->default);
        static::assertNull($restoredTable->columns[0]->identityGeneration);
        static::assertNull($restoredTable->columns[0]->generationExpression);
        static::assertNull($restoredTable->columns[0]->ordinalPosition);
    }

    public function test_round_trip_preserves_procedure(): void
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
        static::assertSame('cleanup_proc', $restoredProc->name);
        static::assertSame(['integer', 'text'], $restoredProc->argumentTypes);
        static::assertSame('plpgsql', $restoredProc->language);
        static::assertSame('BEGIN DELETE FROM logs WHERE age > $1; END;', $restoredProc->definition);
    }

    public function test_round_trip_preserves_sequence_all_fields(): void
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
        static::assertSame('custom_seq', $restoredSeq->name);
        static::assertSame('integer', $restoredSeq->dataType);
        static::assertSame(100, $restoredSeq->startValue);
        static::assertSame(1, $restoredSeq->minValue);
        static::assertSame(999999, $restoredSeq->maxValue);
        static::assertSame(5, $restoredSeq->incrementBy);
        static::assertTrue($restoredSeq->cycle);
        static::assertSame(10, $restoredSeq->cacheValue);
        static::assertSame('users', $restoredSeq->ownedByTable);
        static::assertSame('id', $restoredSeq->ownedByColumn);
    }

    public function test_round_trip_preserves_strict_function(): void
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
        static::assertSame('my_func', $restoredFunc->name);
        static::assertSame('boolean', $restoredFunc->returnType);
        static::assertSame(['text', 'integer'], $restoredFunc->argumentTypes);
        static::assertSame('plpgsql', $restoredFunc->language);
        static::assertTrue($restoredFunc->isStrict);
        static::assertSame(FunctionVolatility::IMMUTABLE, $restoredFunc->volatility);
    }

    public function test_round_trip_preserves_view(): void
    {
        $view = new View('my_view', 'SELECT id, name FROM users', isUpdatable: true);

        $catalog = new Catalog([new Schema('public', views: [$view])]);
        $restored = Catalog::fromArray($catalog->normalize());

        $restoredView = $restored->get('public')->views[0];
        static::assertSame('my_view', $restoredView->name);
        static::assertSame('SELECT id, name FROM users', $restoredView->definition);
        static::assertTrue($restoredView->isUpdatable);
    }
}
