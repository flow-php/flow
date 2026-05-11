<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint as SchemaCheckConstraint;
use Flow\PostgreSql\Schema\Constraint\ForeignKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint as SchemaUniqueConstraint;
use Flow\PostgreSql\Schema\Diff\ColumnDiff;
use Flow\PostgreSql\Schema\Diff\TableDiff;
use Flow\PostgreSql\Schema\IdentityGeneration;
use Flow\PostgreSql\Schema\PartitionStrategy;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_check;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_exclude;
use function Flow\PostgreSql\DSL\schema_foreign_key;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_table;
use function Flow\PostgreSql\DSL\schema_trigger;
use function Flow\PostgreSql\DSL\schema_unique;

final class TableDiffTest extends TestCase
{
    public function test_adds_check_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedCheckConstraints: [schema_check('age > 0', 'chk_users_age')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users ADD CONSTRAINT chk_users_age CHECK (age > 0)', $sqls[0]->toSql());
    }

    public function test_adds_check_constraint_with_no_inherit(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, addedCheckConstraints: [new SchemaCheckConstraint(
            'age > 0',
            'chk_age',
            true,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertStringContainsString('NO INHERIT', $sqls[0]->toSql());
    }

    public function test_adds_column(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);

        $diff = new TableDiff($source, $target, [schema_column_text('name')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users ADD COLUMN name pg_catalog.text', $sqls[0]->toSql());
    }

    public function test_adds_column_with_generated_expression(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, [new Column(
            'full_name',
            ColumnType::text(),
            true,
            isGenerated: true,
            generationExpression: "first_name || ' ' || last_name",
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertStringContainsString('ADD COLUMN', $sqls[0]->toSql());
    }

    public function test_adds_column_with_identity(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, [new Column(
            'seq_id',
            ColumnType::integer(),
            false,
            isIdentity: true,
            identityGeneration: IdentityGeneration::ALWAYS,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertStringContainsString('ADD COLUMN', $sqls[0]->toSql());
    }

    public function test_adds_column_with_not_null_and_default(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [
            schema_column_integer('id', false),
            schema_column_text('status', false, 'active'),
        ]);

        $diff = new TableDiff($source, $target, [schema_column_text('status', false, 'active')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            "ALTER TABLE public.users ADD COLUMN status pg_catalog.text NOT NULL DEFAULT 'active'",
            $sqls[0]->toSql(),
        );
    }

    public function test_adds_deferrable_foreign_key(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, addedForeignKeys: [new ForeignKey(
            'fk_dept',
            ['dept_id'],
            'public',
            'departments',
            ['id'],
            ReferentialAction::NO_ACTION,
            ReferentialAction::CASCADE,
            true,
            true,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertStringContainsString('DEFERRABLE', $sqls[0]->toSql());
        static::assertStringContainsString('INITIALLY DEFERRED', $sqls[0]->toSql());
    }

    public function test_adds_exclude_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedExcludeConstraints: [schema_exclude(
            'USING gist (tsrange(start_date, end_date) WITH &&)',
            'excl_dates_overlap',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'ALTER TABLE public.users ADD CONSTRAINT excl_dates_overlap EXCLUDE USING gist (tsrange(start_date, end_date) WITH &&)',
            $sqls[0]->toSql(),
        );
    }

    public function test_adds_exclude_constraint_deferrable_initially_deferred(): void
    {
        $table = schema_table('bookings', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedExcludeConstraints: [schema_exclude(
            'USING btree (room_id WITH =) DEFERRABLE INITIALLY DEFERRED',
            'exc_room_deferred',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'ALTER TABLE public.bookings ADD CONSTRAINT exc_room_deferred EXCLUDE (room_id WITH =) DEFERRABLE INITIALLY DEFERRED',
            $sqls[0]->toSql(),
        );
    }

    public function test_adds_exclude_constraint_with_predicate(): void
    {
        $table = schema_table('bookings', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedExcludeConstraints: [schema_exclude(
            "USING btree (room_id WITH =) WHERE (status = 'active')",
            'exc_room_active',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            "ALTER TABLE public.bookings ADD CONSTRAINT exc_room_active EXCLUDE (room_id WITH =) WHERE (status = 'active')",
            $sqls[0]->toSql(),
        );
    }

    public function test_adds_foreign_key(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedForeignKeys: [schema_foreign_key(
            ['department_id'],
            'departments',
            ['id'],
            'fk_users_department',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'ALTER TABLE public.users ADD CONSTRAINT fk_users_department FOREIGN KEY (department_id) REFERENCES public.departments (id)',
            $sqls[0]->toSql(),
        );
    }

    public function test_adds_index(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);

        $diff = new TableDiff($table, $table, addedIndexes: [schema_index('idx_users_name', ['name'])]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE INDEX idx_users_name ON public.users (name)', $sqls[0]->toSql());
    }

    public function test_adds_inherit(): void
    {
        $source = schema_table('employees', [schema_column_integer('id', false)]);
        $target = schema_table('employees', [schema_column_integer('id', false)], inherits: ['persons']);

        $diff = new TableDiff($source, $target, addedInherits: ['persons']);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.employees INHERIT persons', $sqls[0]->toSql());
    }

    public function test_adds_primary_key(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedPrimaryKey: schema_primary_key(['id'], 'pk_users'));

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users ADD CONSTRAINT pk_users PRIMARY KEY (id)', $sqls[0]->toSql());
    }

    public function test_adds_trigger(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedTriggers: [schema_trigger(
            'trg_users_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'audit_function',
            true,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_users_audit AFTER INSERT ON public.users FOR EACH ROW EXECUTE FUNCTION audit_function()',
            $sqls[0]->toSql(),
        );
    }

    public function test_adds_trigger_with_before_timing(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedTriggers: [schema_trigger(
            'trg_users_validate',
            'users',
            TriggerTiming::BEFORE,
            [TriggerEvent::INSERT, TriggerEvent::UPDATE],
            'validate_function',
            true,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE TRIGGER trg_users_validate BEFORE INSERT OR UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION validate_function()',
            $sqls[0]->toSql(),
        );
    }

    public function test_adds_unique_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedUniqueConstraints: [schema_unique(['email'], 'uq_users_email')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users ADD CONSTRAINT uq_users_email UNIQUE (email)', $sqls[0]->toSql());
    }

    public function test_adds_unique_constraint_with_nulls_not_distinct(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, addedUniqueConstraints: [new SchemaUniqueConstraint(
            ['email'],
            'uq_email',
            true,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertStringContainsString('NULLS NOT DISTINCT', $sqls[0]->toSql());
    }

    public function test_changes_tablespace(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)], tablespace: 'fast_storage');

        $diff = new TableDiff($source, $target, tablespaceChanged: true);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users SET TABLESPACE fast_storage', $sqls[0]->toSql());
    }

    public function test_delegates_to_column_diff(): void
    {
        $source = schema_table('users', [schema_column_text('name')]);
        $target = schema_table('users', [schema_column_integer('name', false)]);

        $diff = new TableDiff($source, $target, modifiedColumns: [new ColumnDiff(
            'public.users',
            schema_column_text('name'),
            schema_column_integer('name', false),
        )]);

        $sqls = $diff->generate();

        static::assertCount(2, $sqls);
        static::assertSame('ALTER TABLE public.users ALTER COLUMN name TYPE int', $sqls[0]->toSql());
        static::assertSame('ALTER TABLE public.users ALTER COLUMN name SET NOT NULL', $sqls[1]->toSql());
    }

    public function test_dependency_order(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]);

        $diff = new TableDiff(
            $source,
            $target,
            [schema_column_text('email')],
            addedForeignKeys: [schema_foreign_key(['email'], 'emails', ['address'], 'fk_users_email')],
            removedForeignKeys: [schema_foreign_key(['department_id'], 'departments', ['id'], 'fk_users_dept')],
        );

        $sqls = $diff->generate();

        static::assertCount(3, $sqls);
        static::assertSame('ALTER TABLE public.users DROP CONSTRAINT fk_users_dept', $sqls[0]->toSql());
        static::assertSame('ALTER TABLE public.users ADD COLUMN email pg_catalog.text', $sqls[1]->toSql());
        static::assertSame(
            'ALTER TABLE public.users ADD CONSTRAINT fk_users_email FOREIGN KEY (email) REFERENCES public.emails (address)',
            $sqls[2]->toSql(),
        );
    }

    public function test_does_not_require_view_rebuild_when_only_column_added(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);

        $diff = new TableDiff($source, $target, addedColumns: [schema_column_text('name')]);

        static::assertFalse($diff->requiresViewRebuild());
    }

    public function test_does_not_require_view_rebuild_when_only_default_changed(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_integer('age')]);
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_integer('age', default: 0)]);

        $diff = new TableDiff($source, $target, modifiedColumns: [new ColumnDiff(
            'public.users',
            schema_column_integer('age'),
            schema_column_integer('age', default: 0),
        )]);

        static::assertFalse($diff->requiresViewRebuild());
    }

    public function test_does_not_require_view_rebuild_when_only_nullable_changed(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);
        $target = schema_table('users', [
            schema_column_integer('id', false),
            schema_column_text('name', nullable: false),
        ]);

        $diff = new TableDiff($source, $target, modifiedColumns: [new ColumnDiff(
            'public.users',
            schema_column_text('name'),
            schema_column_text('name', nullable: false),
        )]);

        static::assertFalse($diff->requiresViewRebuild());
    }

    public function test_drops_check_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedCheckConstraints: [schema_check('age > 0', 'chk_users_age')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users DROP CONSTRAINT chk_users_age', $sqls[0]->toSql());
    }

    public function test_drops_column(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, removedColumns: [schema_column_text('name')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users DROP name', $sqls[0]->toSql());
    }

    public function test_drops_exclude_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedExcludeConstraints: [schema_exclude(
            'USING gist (tsrange(start_date, end_date) WITH &&)',
            'excl_dates_overlap',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users DROP CONSTRAINT excl_dates_overlap', $sqls[0]->toSql());
    }

    public function test_drops_foreign_key(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedForeignKeys: [schema_foreign_key(
            ['department_id'],
            'departments',
            ['id'],
            'fk_users_department',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users DROP CONSTRAINT fk_users_department', $sqls[0]->toSql());
    }

    public function test_drops_index(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedIndexes: [schema_index('idx_users_name', ['name'])]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP INDEX idx_users_name', $sqls[0]->toSql());
    }

    public function test_drops_primary_key(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedPrimaryKey: schema_primary_key(['id'], 'pk_users'));

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users DROP CONSTRAINT pk_users', $sqls[0]->toSql());
    }

    public function test_drops_trigger(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedTriggers: [schema_trigger(
            'trg_users_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'audit_function',
            true,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP TRIGGER trg_users_audit ON public.users', $sqls[0]->toSql());
    }

    public function test_drops_unique_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedUniqueConstraints: [schema_unique(['email'], 'uq_users_email')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users DROP CONSTRAINT uq_users_email', $sqls[0]->toSql());
    }

    public function test_empty_diff(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table);

        static::assertTrue($diff->isEmpty());
    }

    public function test_not_empty_when_column_added(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [
            schema_column_integer('id', false),
            schema_column_text('name'),
        ]);

        $diff = new TableDiff($source, $target, [schema_column_text('name')]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_not_empty_when_column_modified(): void
    {
        $source = schema_table('users', [schema_column_text('name')]);
        $target = schema_table('users', [schema_column_text('name', false)]);

        $diff = new TableDiff($source, $target, modifiedColumns: [new ColumnDiff(
            'public.users',
            schema_column_text('name'),
            schema_column_text('name', false),
        )]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_not_empty_when_inherits_added(): void
    {
        $source = schema_table('employees', [schema_column_integer('id', false)]);
        $target = schema_table('employees', [schema_column_integer('id', false)], inherits: ['persons']);

        $diff = new TableDiff($source, $target, addedInherits: ['persons']);

        static::assertFalse($diff->isEmpty());
    }

    public function test_not_empty_when_inherits_removed(): void
    {
        $source = schema_table('employees', [schema_column_integer('id', false)], inherits: ['persons']);
        $target = schema_table('employees', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, removedInherits: ['persons']);

        static::assertFalse($diff->isEmpty());
    }

    public function test_not_empty_when_partition_changed(): void
    {
        $source = schema_table('events', [schema_column_integer('id', false)]);
        $target = schema_table(
            'events',
            [schema_column_integer('id', false)],
            partitionStrategy: PartitionStrategy::RANGE,
            partitionColumns: ['id'],
        );

        $diff = new TableDiff($source, $target, partitionChanged: true);

        static::assertFalse($diff->isEmpty());
    }

    public function test_not_empty_when_tablespace_changed(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)], tablespace: 'fast_storage');

        $diff = new TableDiff($source, $target, tablespaceChanged: true);

        static::assertFalse($diff->isEmpty());
    }

    public function test_not_empty_when_unlogged_changed(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)], unlogged: true);

        $diff = new TableDiff($source, $target, unloggedChanged: true);

        static::assertFalse($diff->isEmpty());
    }

    public function test_partition_changed_throws_runtime_exception(): void
    {
        $source = schema_table('events', [schema_column_integer('id', false)]);
        $target = schema_table(
            'events',
            [schema_column_integer('id', false)],
            partitionStrategy: PartitionStrategy::RANGE,
            partitionColumns: ['id'],
        );

        $diff = new TableDiff($source, $target, partitionChanged: true);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage(
            'Partition strategy change on table "public.events" cannot be applied via ALTER TABLE',
        );

        $diff->generate();
    }

    public function test_removes_inherit(): void
    {
        $source = schema_table('employees', [schema_column_integer('id', false)], inherits: ['persons']);
        $target = schema_table('employees', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, removedInherits: ['persons']);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.employees NO INHERIT persons', $sqls[0]->toSql());
    }

    public function test_renames_index(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, renamedIndexes: ['idx_users_name' => schema_index('idx_users_email', [
            'email',
        ])]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER INDEX public.idx_users_name RENAME TO idx_users_email', $sqls[0]->toSql());
    }

    public function test_requires_view_rebuild_when_column_dropped(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, removedColumns: [schema_column_text('name')]);

        static::assertTrue($diff->requiresViewRebuild());
    }

    public function test_requires_view_rebuild_when_column_renamed(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_text('full_name')]);

        $diff = new TableDiff($source, $target, modifiedColumns: [new ColumnDiff(
            'public.users',
            schema_column_text('name'),
            schema_column_text('full_name'),
        )]);

        static::assertTrue($diff->requiresViewRebuild());
    }

    public function test_requires_view_rebuild_when_column_type_changes(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_integer('amount')]);
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_text('amount')]);

        $diff = new TableDiff($source, $target, modifiedColumns: [new ColumnDiff(
            'public.users',
            schema_column_integer('amount'),
            schema_column_text('amount'),
        )]);

        static::assertTrue($diff->requiresViewRebuild());
    }

    public function test_returns_empty_when_no_changes(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table);

        static::assertSame([], $diff->generate());
    }

    public function test_reversed_add_column(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($target, $source, removedColumns: [schema_column_text('name')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users DROP name', $sqls[0]->toSql());
    }

    public function test_reversed_add_index(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedIndexes: [schema_index('idx_name', ['name'])]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP INDEX idx_name', $sqls[0]->toSql());
    }

    public function test_reversed_add_primary_key(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedPrimaryKey: schema_primary_key(['id'], 'pk_users'));

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users DROP CONSTRAINT pk_users', $sqls[0]->toSql());
    }

    public function test_reversed_add_trigger(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedTriggers: [schema_trigger(
            'trg_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'audit_fn',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertStringContainsString('DROP TRIGGER trg_audit', $sqls[0]->toSql());
    }

    public function test_reversed_drop_check_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedCheckConstraints: [schema_check('age > 0', 'chk_age')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertStringContainsString('CHECK', $sqls[0]->toSql());
    }

    public function test_reversed_drop_foreign_key(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, addedForeignKeys: [schema_foreign_key(
            ['dept_id'],
            'departments',
            ['id'],
            'fk_dept',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertStringContainsString('FOREIGN KEY', $sqls[0]->toSql());
    }

    public function test_reversed_inherit_changes(): void
    {
        $source = schema_table('employees', [schema_column_integer('id', false)], inherits: ['persons']);
        $target = schema_table('employees', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, removedInherits: ['persons']);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.employees NO INHERIT persons', $sqls[0]->toSql());
    }

    public function test_reversed_modified_column(): void
    {
        $source = schema_table('users', [schema_column_integer('name', false)]);
        $target = schema_table('users', [schema_column_text('name')]);

        $diff = new TableDiff($source, $target, modifiedColumns: [new ColumnDiff(
            'public.users',
            schema_column_integer('name', false),
            schema_column_text('name'),
        )]);

        $sqls = $diff->generate();

        static::assertCount(2, $sqls);
        static::assertStringContainsString('TYPE', $sqls[0]->toSql());
    }

    public function test_reversed_unlogged_change(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)], unlogged: true);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, unloggedChanged: true);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users SET LOGGED', $sqls[0]->toSql());
    }

    public function test_sets_logged(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)], unlogged: true);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($source, $target, unloggedChanged: true);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users SET LOGGED', $sqls[0]->toSql());
    }

    public function test_sets_unlogged(): void
    {
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)], unlogged: true);

        $diff = new TableDiff($source, $target, unloggedChanged: true);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users SET UNLOGGED', $sqls[0]->toSql());
    }

    public function test_throws_when_dropping_unnamed_check_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedCheckConstraints: [schema_check('age > 0')]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot drop unnamed check constraint on table "public.users"');

        $diff->generate();
    }

    public function test_throws_when_dropping_unnamed_exclude_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedExcludeConstraints: [schema_exclude('USING gist (col WITH &&)')]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot drop unnamed exclude constraint on table "public.users"');

        $diff->generate();
    }

    public function test_throws_when_dropping_unnamed_foreign_key(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedForeignKeys: [schema_foreign_key(
            ['department_id'],
            'departments',
            ['id'],
        )]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot drop unnamed foreign key on table "public.users"');

        $diff->generate();
    }

    public function test_throws_when_dropping_unnamed_primary_key(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedPrimaryKey: schema_primary_key(['id']));

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot drop unnamed primary key on table "public.users"');

        $diff->generate();
    }

    public function test_throws_when_dropping_unnamed_unique_constraint(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new TableDiff($table, $table, removedUniqueConstraints: [schema_unique(['email'])]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot drop unnamed unique constraint on table "public.users"');

        $diff->generate();
    }
}
