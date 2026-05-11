<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use Flow\PostgreSql\Schema\Exception\ColumnNotFoundException;
use Flow\PostgreSql\Schema\IdentityGeneration;
use Flow\PostgreSql\Schema\IndexMethod;
use Flow\PostgreSql\Schema\PartitionStrategy;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_jsonb;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_timestamptz;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\schema_check;
use function Flow\PostgreSql\DSL\schema_column;
use function Flow\PostgreSql\DSL\schema_foreign_key;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_table;
use function Flow\PostgreSql\DSL\schema_trigger;
use function Flow\PostgreSql\DSL\schema_unique;

final class TableTest extends TestCase
{
    public function test_column_lookup(): void
    {
        $table = schema_table('users', [
            schema_column('id', column_type_integer(), nullable: false),
            schema_column('name', column_type_varchar(255)),
        ]);

        static::assertSame('name', $table->column('name')->name);
    }

    public function test_column_names(): void
    {
        $table = schema_table('users', [
            schema_column('id', column_type_integer(), nullable: false),
            schema_column('name', column_type_varchar(255)),
        ]);

        static::assertSame(['id', 'name'], $table->columnNames());
    }

    public function test_column_not_found_throws(): void
    {
        $table = schema_table('users', [
            schema_column('id', column_type_integer(), nullable: false),
        ]);

        $this->expectException(ColumnNotFoundException::class);
        $table->column('missing');
    }

    public function test_has_column(): void
    {
        $table = schema_table('users', [
            schema_column('id', column_type_integer(), nullable: false),
        ]);

        static::assertTrue($table->hasColumn('id'));
        static::assertFalse($table->hasColumn('missing'));
    }

    public function test_qualified_name(): void
    {
        $table = schema_table('users', [
            schema_column('id', column_type_integer(), nullable: false),
        ]);

        static::assertSame('public.users', $table->qualifiedName());
    }

    public function test_table_construction(): void
    {
        $table = schema_table('users', [
            schema_column('id', column_type_integer(), nullable: false),
            schema_column('name', column_type_varchar(255)),
        ]);

        static::assertSame('public', $table->schema);
        static::assertSame('users', $table->name);
        static::assertCount(2, $table->columns);
    }

    public function test_table_with_foreign_keys(): void
    {
        $table = schema_table(
            'posts',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('user_id', column_type_integer(), nullable: false),
            ],
            foreignKeys: [
                schema_foreign_key(['user_id'], 'users', ['id']),
            ],
        );

        static::assertCount(1, $table->foreignKeys);
        static::assertSame('users', $table->foreignKeys[0]->referenceTable);
    }

    public function test_table_with_indexes(): void
    {
        $table = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('email', column_type_varchar(255)),
            ],
            indexes: [
                schema_index('idx_users_email', ['email'], unique: true),
            ],
        );

        static::assertCount(1, $table->indexes);
        static::assertSame('idx_users_email', $table->indexes[0]->name);
        static::assertTrue($table->indexes[0]->unique);
    }

    public function test_table_with_primary_key(): void
    {
        $table = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            primaryKey: schema_primary_key(['id']),
        );

        static::assertNotNull($table->primaryKey);
        static::assertSame(['id'], $table->primaryKey->columns);
    }

    public function test_table_with_triggers(): void
    {
        $table = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('updated_at', column_type_timestamptz()),
            ],
            triggers: [
                schema_trigger(
                    'trg_update_timestamp',
                    'users',
                    TriggerTiming::BEFORE,
                    [TriggerEvent::UPDATE],
                    'update_timestamp',
                    forEachRow: true,
                ),
            ],
        );

        static::assertCount(1, $table->triggers);
        static::assertSame('trg_update_timestamp', $table->triggers[0]->name);
    }

    public function test_to_sql_generates_create_table(): void
    {
        $sqls = schema_table('users', [
            schema_column('id', column_type_integer(), nullable: false),
            schema_column('name', column_type_varchar(255)),
        ])->toSql();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE TABLE public.users (id int NOT NULL, name varchar(255))', $sqls[0]->toSql());
    }

    public function test_to_sql_generates_create_table_with_indexes(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('email', column_type_varchar(255)),
            ],
            indexes: [
                schema_index('idx_users_email', ['email'], unique: true),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame('CREATE TABLE public.users (id int NOT NULL, email varchar(255))', $sqls[0]->toSql());
        static::assertSame('CREATE UNIQUE INDEX idx_users_email ON public.users (email)', $sqls[1]->toSql());
    }

    public function test_to_sql_generates_create_table_with_primary_key(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            primaryKey: schema_primary_key(['id']),
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE TABLE public.users (id int NOT NULL, PRIMARY KEY (id))', $sqls[0]->toSql());
    }

    public function test_to_sql_generates_inherits(): void
    {
        $sqls = schema_table(
            'employees',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('name', column_type_varchar(255)),
            ],
            inherits: ['persons'],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.employees (id int NOT NULL, name varchar(255)) INHERITS (persons)',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_generates_partition_by_hash(): void
    {
        $sqls = schema_table(
            'events',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            partitionStrategy: PartitionStrategy::HASH,
            partitionColumns: ['id'],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE TABLE public.events (id int NOT NULL) PARTITION BY HASH (id)', $sqls[0]->toSql());
    }

    public function test_to_sql_generates_partition_by_list(): void
    {
        $sqls = schema_table(
            'events',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('region', column_type_varchar(50), nullable: false),
            ],
            partitionStrategy: PartitionStrategy::LIST,
            partitionColumns: ['region'],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.events (id int NOT NULL, region varchar(50) NOT NULL) PARTITION BY LIST (region)',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_generates_partition_by_range(): void
    {
        $sqls = schema_table(
            'events',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('name', column_type_varchar(255), nullable: false),
            ],
            partitionStrategy: PartitionStrategy::RANGE,
            partitionColumns: ['name'],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.events (id int NOT NULL, name varchar(255) NOT NULL) PARTITION BY RANGE (name)',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_generates_partition_with_tablespace(): void
    {
        $sqls = schema_table(
            'events',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('name', column_type_varchar(255), nullable: false),
            ],
            partitionStrategy: PartitionStrategy::RANGE,
            partitionColumns: ['name'],
            tablespace: 'fast_storage',
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.events (id int NOT NULL, name varchar(255) NOT NULL) PARTITION BY RANGE (name) TABLESPACE fast_storage',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_generates_tablespace(): void
    {
        $sqls = schema_table(
            'events',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            tablespace: 'fast_storage',
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE TABLE public.events (id int NOT NULL) TABLESPACE fast_storage', $sqls[0]->toSql());
    }

    public function test_to_sql_generates_unlogged_table(): void
    {
        $sqls = schema_table(
            'events',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('name', column_type_varchar(255)),
            ],
            unlogged: true,
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE UNLOGGED TABLE public.events (id int NOT NULL, name varchar(255))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_all_constraint_types(): void
    {
        $sqls = schema_table(
            'orders',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('user_id', column_type_integer(), nullable: false),
                schema_column('amount', column_type_integer(), nullable: false),
                schema_column('email', column_type_varchar(255), nullable: false),
            ],
            primaryKey: schema_primary_key(['id']),
            indexes: [
                schema_index('idx_orders_user_id', ['user_id']),
            ],
            foreignKeys: [
                schema_foreign_key(['user_id'], 'users', ['id'], onDelete: ReferentialAction::CASCADE),
            ],
            uniqueConstraints: [
                schema_unique(['email']),
            ],
            checkConstraints: [
                schema_check('amount > 0'),
            ],
            triggers: [
                schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_function'),
            ],
        )->toSql();

        static::assertCount(3, $sqls);
        static::assertSame(
            'CREATE TABLE public.orders (id int NOT NULL, user_id int NOT NULL, amount int NOT NULL, email varchar(255) NOT NULL, PRIMARY KEY (id), UNIQUE (email), CHECK (amount > 0), FOREIGN KEY (user_id) REFERENCES public.users (id) ON DELETE CASCADE)',
            $sqls[0]->toSql(),
        );
        static::assertSame('CREATE INDEX idx_orders_user_id ON public.orders (user_id)', $sqls[1]->toSql());
        static::assertSame(
            'CREATE TRIGGER trg_audit AFTER INSERT ON public.orders EXECUTE FUNCTION audit_function()',
            $sqls[2]->toSql(),
        );
    }

    public function test_to_sql_with_check_constraint(): void
    {
        $sqls = schema_table(
            'products',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('price', column_type_integer(), nullable: false),
            ],
            checkConstraints: [
                schema_check('price > 0'),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.products (id int NOT NULL, price int NOT NULL, CHECK (price > 0))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_check_constraint_no_inherit(): void
    {
        $sqls = schema_table(
            'products',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('price', column_type_integer(), nullable: false),
            ],
            checkConstraints: [
                schema_check('price > 0', noInherit: true),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.products (id int NOT NULL, price int NOT NULL, CHECK (price > 0) NO INHERIT)',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_column_default(): void
    {
        $sqls = schema_table('users', [
            schema_column('id', column_type_integer(), nullable: false),
            schema_column('status', column_type_varchar(50), default: 'active'),
        ])->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            "CREATE TABLE public.users (id int NOT NULL, status varchar(50) DEFAULT 'active')",
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_column_default_numeric(): void
    {
        $sqls = schema_table('products', [
            schema_column('id', column_type_integer(), nullable: false),
            schema_column('quantity', column_type_integer(), default: 0),
        ])->toSql();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE TABLE public.products (id int NOT NULL, quantity int DEFAULT 0)', $sqls[0]->toSql());
    }

    public function test_to_sql_with_column_generated_expression(): void
    {
        $sqls = schema_table('users', [
            schema_column('first_name', column_type_varchar(100)),
            schema_column('last_name', column_type_varchar(100)),
            schema_column(
                'full_name',
                column_type_text(),
                isGenerated: true,
                generationExpression: "first_name || ' ' || last_name",
            ),
        ])->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            "CREATE TABLE public.users (first_name varchar(100), last_name varchar(100), full_name pg_catalog.text GENERATED ALWAYS AS ((first_name || ' ') || last_name) STORED)",
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_column_identity_always(): void
    {
        $sqls = schema_table('users', [
            schema_column(
                'id',
                column_type_integer(),
                nullable: false,
                isIdentity: true,
                identityGeneration: IdentityGeneration::ALWAYS,
            ),
            schema_column('name', column_type_varchar(255)),
        ])->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.users (id int NOT NULL GENERATED ALWAYS AS IDENTITY, name varchar(255))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_composite_unique_constraint(): void
    {
        $sqls = schema_table(
            'user_roles',
            [
                schema_column('user_id', column_type_integer(), nullable: false),
                schema_column('role_id', column_type_integer(), nullable: false),
            ],
            uniqueConstraints: [
                schema_unique(['user_id', 'role_id'], name: 'uq_user_role'),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.user_roles (user_id int NOT NULL, role_id int NOT NULL, CONSTRAINT uq_user_role UNIQUE (user_id, role_id))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_custom_schema(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            schema: 'myschema',
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE TABLE myschema.users (id int NOT NULL)', $sqls[0]->toSql());
    }

    public function test_to_sql_with_deferrable_foreign_key(): void
    {
        $sqls = schema_table(
            'orders',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('user_id', column_type_integer(), nullable: false),
            ],
            foreignKeys: [
                schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true, initiallyDeferred: true),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.orders (id int NOT NULL, user_id int NOT NULL, FOREIGN KEY (user_id) REFERENCES public.users (id) DEFERRABLE INITIALLY DEFERRED)',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_foreign_key_constraint(): void
    {
        $sqls = schema_table(
            'orders',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('user_id', column_type_integer(), nullable: false),
            ],
            foreignKeys: [
                schema_foreign_key(
                    ['user_id'],
                    'users',
                    ['id'],
                    onDelete: ReferentialAction::CASCADE,
                    onUpdate: ReferentialAction::RESTRICT,
                ),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.orders (id int NOT NULL, user_id int NOT NULL, FOREIGN KEY (user_id) REFERENCES public.users (id) ON UPDATE RESTRICT ON DELETE CASCADE)',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_foreign_key_custom_reference_schema(): void
    {
        $sqls = schema_table(
            'orders',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('product_id', column_type_integer(), nullable: false),
            ],
            foreignKeys: [
                schema_foreign_key(['product_id'], 'products', ['id'], referenceSchema: 'catalog'),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.orders (id int NOT NULL, product_id int NOT NULL, FOREIGN KEY (product_id) REFERENCES catalog.products (id))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_foreign_key_default_actions(): void
    {
        $sqls = schema_table(
            'orders',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('user_id', column_type_integer(), nullable: false),
            ],
            foreignKeys: [
                schema_foreign_key(['user_id'], 'users', ['id']),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.orders (id int NOT NULL, user_id int NOT NULL, FOREIGN KEY (user_id) REFERENCES public.users (id))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_foreign_key_set_null_on_delete(): void
    {
        $sqls = schema_table(
            'comments',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('user_id', column_type_integer()),
            ],
            foreignKeys: [
                schema_foreign_key(['user_id'], 'users', ['id'], onDelete: ReferentialAction::SET_NULL),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.comments (id int NOT NULL, user_id int, FOREIGN KEY (user_id) REFERENCES public.users (id) ON DELETE SET NULL)',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_gin_index(): void
    {
        $sqls = schema_table(
            'documents',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('metadata', column_type_jsonb()),
            ],
            indexes: [
                schema_index('idx_documents_metadata', ['metadata'], method: IndexMethod::GIN),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE INDEX idx_documents_metadata ON public.documents USING gin (metadata)',
            $sqls[1]->toSql(),
        );
    }

    public function test_to_sql_with_gist_index(): void
    {
        $sqls = schema_table(
            'events',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('range_col', column_type_text()),
            ],
            indexes: [
                schema_index('idx_events_range', ['range_col'], method: IndexMethod::GIST),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame('CREATE INDEX idx_events_range ON public.events USING gist (range_col)', $sqls[1]->toSql());
    }

    public function test_to_sql_with_hash_index(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('email', column_type_varchar(255)),
            ],
            indexes: [
                schema_index('idx_users_email_hash', ['email'], method: IndexMethod::HASH),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame('CREATE INDEX idx_users_email_hash ON public.users USING hash (email)', $sqls[1]->toSql());
    }

    public function test_to_sql_with_multiple_indexes(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('email', column_type_varchar(255)),
                schema_column('name', column_type_varchar(255)),
            ],
            indexes: [
                schema_index('idx_users_email', ['email'], unique: true),
                schema_index('idx_users_name', ['name']),
            ],
        )->toSql();

        static::assertCount(3, $sqls);
        static::assertSame('CREATE UNIQUE INDEX idx_users_email ON public.users (email)', $sqls[1]->toSql());
        static::assertSame('CREATE INDEX idx_users_name ON public.users (name)', $sqls[2]->toSql());
    }

    public function test_to_sql_with_multiple_triggers(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            triggers: [
                schema_trigger(
                    'trg_before',
                    'users',
                    TriggerTiming::BEFORE,
                    [TriggerEvent::INSERT],
                    'before_func',
                    forEachRow: true,
                ),
                schema_trigger('trg_after', 'users', TriggerTiming::AFTER, [TriggerEvent::UPDATE], 'after_func'),
            ],
        )->toSql();

        static::assertCount(3, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_before BEFORE INSERT ON public.users FOR EACH ROW EXECUTE FUNCTION before_func()',
            $sqls[1]->toSql(),
        );
        static::assertSame(
            'CREATE TRIGGER trg_after AFTER UPDATE ON public.users EXECUTE FUNCTION after_func()',
            $sqls[2]->toSql(),
        );
    }

    public function test_to_sql_with_named_check_constraint(): void
    {
        $sqls = schema_table(
            'products',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('price', column_type_integer(), nullable: false),
            ],
            checkConstraints: [
                schema_check('price > 0', name: 'positive_price'),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.products (id int NOT NULL, price int NOT NULL, CONSTRAINT positive_price CHECK (price > 0))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_named_foreign_key_constraint(): void
    {
        $sqls = schema_table(
            'orders',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('user_id', column_type_integer(), nullable: false),
            ],
            foreignKeys: [
                schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders_user_id'),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.orders (id int NOT NULL, user_id int NOT NULL, CONSTRAINT fk_orders_user_id FOREIGN KEY (user_id) REFERENCES public.users (id))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_named_primary_key(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            primaryKey: schema_primary_key(['id'], name: 'pk_users'),
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.users (id int NOT NULL, CONSTRAINT pk_users PRIMARY KEY (id))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_named_unique_constraint(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('email', column_type_varchar(255), nullable: false),
            ],
            uniqueConstraints: [
                schema_unique(['email'], name: 'uq_users_email'),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.users (id int NOT NULL, email varchar(255) NOT NULL, CONSTRAINT uq_users_email UNIQUE (email))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_non_unique_index(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('name', column_type_varchar(255)),
            ],
            indexes: [
                schema_index('idx_users_name', ['name']),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame('CREATE INDEX idx_users_name ON public.users (name)', $sqls[1]->toSql());
    }

    public function test_to_sql_with_trigger_after_insert(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            triggers: [
                schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_function'),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_audit AFTER INSERT ON public.users EXECUTE FUNCTION audit_function()',
            $sqls[1]->toSql(),
        );
    }

    public function test_to_sql_with_trigger_before_delete(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            triggers: [
                schema_trigger(
                    'trg_prevent_delete',
                    'users',
                    TriggerTiming::BEFORE,
                    [TriggerEvent::DELETE],
                    'prevent_delete_func',
                    forEachRow: true,
                ),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_prevent_delete BEFORE DELETE ON public.users FOR EACH ROW EXECUTE FUNCTION prevent_delete_func()',
            $sqls[1]->toSql(),
        );
    }

    public function test_to_sql_with_trigger_before_update_for_each_row(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('updated_at', column_type_timestamptz()),
            ],
            triggers: [
                schema_trigger(
                    'trg_update_timestamp',
                    'users',
                    TriggerTiming::BEFORE,
                    [TriggerEvent::UPDATE],
                    'update_timestamp',
                    forEachRow: true,
                ),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_update_timestamp BEFORE UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION update_timestamp()',
            $sqls[1]->toSql(),
        );
    }

    public function test_to_sql_with_trigger_instead_of(): void
    {
        $sqls = schema_table(
            'users_view',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            triggers: [
                schema_trigger(
                    'trg_view_insert',
                    'users_view',
                    TriggerTiming::INSTEAD_OF,
                    [TriggerEvent::INSERT],
                    'insert_to_users',
                    forEachRow: true,
                ),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_view_insert INSTEAD OF INSERT ON public.users_view FOR EACH ROW EXECUTE FUNCTION insert_to_users()',
            $sqls[1]->toSql(),
        );
    }

    public function test_to_sql_with_trigger_multiple_events(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            triggers: [
                schema_trigger(
                    'trg_audit',
                    'users',
                    TriggerTiming::AFTER,
                    [TriggerEvent::INSERT, TriggerEvent::UPDATE],
                    'audit_function',
                    forEachRow: true,
                ),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_audit AFTER INSERT OR UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION audit_function()',
            $sqls[1]->toSql(),
        );
    }

    public function test_to_sql_with_trigger_without_for_each_row(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
            ],
            triggers: [
                schema_trigger('trg_statement', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'process_batch'),
            ],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_statement AFTER INSERT ON public.users EXECUTE FUNCTION process_batch()',
            $sqls[1]->toSql(),
        );
    }

    public function test_to_sql_with_unique_constraint(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('email', column_type_varchar(255), nullable: false),
            ],
            uniqueConstraints: [
                schema_unique(['email']),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.users (id int NOT NULL, email varchar(255) NOT NULL, UNIQUE (email))',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_with_unique_constraint_nulls_not_distinct(): void
    {
        $sqls = schema_table(
            'users',
            [
                schema_column('id', column_type_integer(), nullable: false),
                schema_column('email', column_type_varchar(255)),
            ],
            uniqueConstraints: [
                schema_unique(['email'], nullsNotDistinct: true),
            ],
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TABLE public.users (id int NOT NULL, email varchar(255), UNIQUE NULLS NOT DISTINCT (email))',
            $sqls[0]->toSql(),
        );
    }
}
