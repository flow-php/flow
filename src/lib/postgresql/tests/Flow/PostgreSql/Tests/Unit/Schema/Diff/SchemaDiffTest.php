<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Diff\SchemaDiff;
use Flow\PostgreSql\Schema\Diff\SequenceDiff;
use Flow\PostgreSql\Schema\Diff\TableDiff;
use Flow\PostgreSql\Schema\Domain;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_domain;
use function Flow\PostgreSql\DSL\schema_extension;
use function Flow\PostgreSql\DSL\schema_function;
use function Flow\PostgreSql\DSL\schema_materialized_view;
use function Flow\PostgreSql\DSL\schema_procedure;
use function Flow\PostgreSql\DSL\schema_sequence;
use function Flow\PostgreSql\DSL\schema_table;
use function Flow\PostgreSql\DSL\schema_view;
use function Flow\PostgreSql\DSL\select;

final class SchemaDiffTest extends TestCase
{
    public function test_creates_added_domain(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedDomains: [schema_domain('email', ColumnType::text(), nullable: false)]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE DOMAIN email AS pg_catalog.text NOT NULL', $sqls[0]->toSql());
    }

    public function test_creates_added_extension(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedExtensions: [schema_extension('uuid-ossp')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE EXTENSION "uuid-ossp"', $sqls[0]->toSql());
    }

    public function test_creates_added_function(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedFunctions: [schema_function(
            'my_func',
            'text',
            ['integer'],
            'sql',
            'SELECT name FROM users WHERE id = $1',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE OR REPLACE FUNCTION my_func(IN int) RETURNS text LANGUAGE sql AS $$SELECT name FROM users WHERE id = $1$$',
            $sqls[0]->toSql(),
        );
    }

    public function test_creates_added_sequence(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedSequences: [schema_sequence('users_id_seq')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE SEQUENCE users_id_seq AS bigint START 1 INCREMENT 1 MINVALUE 1 CACHE 1 NO MAXVALUE',
            $sqls[0]->toSql(),
        );
    }

    public function test_creates_added_table(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, [schema_table('users', [
            schema_column_integer('id', false),
            schema_column_text('name'),
        ])]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE TABLE public.users (id int NOT NULL, name pg_catalog.text)', $sqls[0]->toSql());
    }

    public function test_creates_added_view(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedViews: [schema_view(
            'active_users',
            'SELECT * FROM users WHERE active = true',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE VIEW active_users AS SELECT * FROM users WHERE active = true', $sqls[0]->toSql());
    }

    public function test_delegates_to_table_diff(): void
    {
        $s = schema('public');
        $sourceTable = schema_table('users', [schema_column_integer('id', false)]);
        $targetTable = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);

        $tableDiff = new TableDiff($sourceTable, $targetTable, [new Column('name', ColumnType::text(), true)]);

        $diff = new SchemaDiff($s, $s, modifiedTables: [$tableDiff]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users ADD COLUMN name pg_catalog.text', $sqls[0]->toSql());
    }

    public function test_dependency_order(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedTables: [schema_table('users', [schema_column_integer('id', false)])],
            removedViews: [schema_view('active_users', 'SELECT * FROM users WHERE active = true')],
            addedExtensions: [schema_extension('uuid-ossp')],
        );

        $sqls = $diff->generate();

        static::assertCount(3, $sqls);
        static::assertSame('CREATE EXTENSION "uuid-ossp"', $sqls[0]->toSql());
        static::assertSame('CREATE TABLE public.users (id int NOT NULL)', $sqls[1]->toSql());
        static::assertSame('DROP VIEW active_users', $sqls[2]->toSql());
    }

    public function test_drops_all_removed_object_types(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedTables: [schema_table('users', [schema_column_integer('id', false)])],
            removedSequences: [schema_sequence('users_id_seq')],
            removedViews: [schema_view('active_users', select(literal(1))->toSql())],
            removedMaterializedViews: [schema_materialized_view('mv_stats', select(literal(1))->toSql())],
            removedFunctions: [schema_function('my_func', 'text')],
            removedProcedures: [schema_procedure('cleanup')],
            removedDomains: [schema_domain('email', ColumnType::text())],
            removedExtensions: [schema_extension('pgcrypto')],
        );

        $sqls = $diff->generate();

        static::assertSame('DROP MATERIALIZED VIEW mv_stats', $sqls[0]->toSql());
        static::assertSame('DROP VIEW active_users', $sqls[1]->toSql());
        static::assertSame('DROP TABLE public.users CASCADE', $sqls[2]->toSql());
        static::assertSame('DROP PROCEDURE cleanup', $sqls[3]->toSql());
        static::assertSame('DROP FUNCTION my_func', $sqls[4]->toSql());
        static::assertSame('DROP SEQUENCE users_id_seq', $sqls[5]->toSql());
        static::assertSame('DROP DOMAIN email CASCADE', $sqls[6]->toSql());
        static::assertSame('DROP EXTENSION pgcrypto', $sqls[7]->toSql());
        static::assertCount(8, $sqls);
    }

    public function test_drops_all_removed_object_types_with_if_exists(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedTables: [schema_table('users', [schema_column_integer('id', false)])],
            removedSequences: [schema_sequence('users_id_seq')],
            removedViews: [schema_view('active_users', select(literal(1))->toSql())],
            removedMaterializedViews: [schema_materialized_view('mv_stats', select(literal(1))->toSql())],
            removedFunctions: [schema_function('my_func', 'text')],
            removedProcedures: [schema_procedure('cleanup')],
            removedDomains: [schema_domain('email', ColumnType::text())],
            removedExtensions: [schema_extension('pgcrypto')],
            dropIfExists: true,
        );

        $sqls = $diff->generate();

        static::assertSame('DROP MATERIALIZED VIEW IF EXISTS mv_stats', $sqls[0]->toSql());
        static::assertSame('DROP VIEW IF EXISTS active_users', $sqls[1]->toSql());
        static::assertSame('DROP TABLE IF EXISTS public.users CASCADE', $sqls[2]->toSql());
        static::assertSame('DROP PROCEDURE IF EXISTS cleanup', $sqls[3]->toSql());
        static::assertSame('DROP FUNCTION IF EXISTS my_func', $sqls[4]->toSql());
        static::assertSame('DROP SEQUENCE IF EXISTS users_id_seq', $sqls[5]->toSql());
        static::assertSame('DROP DOMAIN IF EXISTS email CASCADE', $sqls[6]->toSql());
        static::assertSame('DROP EXTENSION IF EXISTS pgcrypto', $sqls[7]->toSql());
        static::assertCount(8, $sqls);
    }

    public function test_drops_removed_domain(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedDomains: [schema_domain('email', ColumnType::text())]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP DOMAIN email CASCADE', $sqls[0]->toSql());
    }

    public function test_drops_removed_extension(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedExtensions: [schema_extension('uuid-ossp')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP EXTENSION "uuid-ossp"', $sqls[0]->toSql());
    }

    public function test_drops_removed_function(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedFunctions: [schema_function('my_func', 'text')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP FUNCTION my_func', $sqls[0]->toSql());
    }

    public function test_drops_removed_materialized_view(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedMaterializedViews: [schema_materialized_view(
            'mv_stats',
            select(literal(1))->toSql(),
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP MATERIALIZED VIEW mv_stats', $sqls[0]->toSql());
    }

    public function test_drops_removed_procedure(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedProcedures: [schema_procedure('cleanup')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP PROCEDURE cleanup', $sqls[0]->toSql());
    }

    public function test_drops_removed_sequence(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedSequences: [schema_sequence('users_id_seq')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP SEQUENCE users_id_seq', $sqls[0]->toSql());
    }

    public function test_drops_removed_table(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedTables: [schema_table('users', [schema_column_integer('id', false)])]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP TABLE public.users CASCADE', $sqls[0]->toSql());
    }

    public function test_drops_removed_view(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedViews: [schema_view(
            'active_users',
            'SELECT * FROM users WHERE active = true',
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP VIEW active_users', $sqls[0]->toSql());
    }

    public function test_is_empty_when_all_arrays_empty(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s);

        static::assertTrue($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_domains(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedDomains: [new Domain('email', ColumnType::text())]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_extensions(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedExtensions: [schema_extension('uuid-ossp')]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_functions(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedFunctions: [schema_function('f', 'void')]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_procedures(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedProcedures: [schema_procedure('p')]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_tables(): void
    {
        $s = schema('public');
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff($s, $s, [$table]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_views(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, addedViews: [schema_view('v', select(literal(1))->toSql())]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_modified_sequences(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, modifiedSequences: [new SequenceDiff(
            schema_sequence('s1'),
            schema_sequence('s1', incrementBy: 5),
        )]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_removed_extensions(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedExtensions: [schema_extension('uuid-ossp')]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_removed_materialized_views(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedMaterializedViews: [schema_materialized_view(
            'mv',
            select(literal(1))->toSql(),
        )]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_removed_tables(): void
    {
        $s = schema('public');
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff($s, $s, removedTables: [$table]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_renamed_tables(): void
    {
        $s = schema('public');
        $table = schema_table('new_users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff($s, $s, renamedTables: ['public.old_users' => $table]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_renames_table(): void
    {
        $s = schema('public');
        $table = schema_table('new_users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff($s, $s, renamedTables: ['public.old_users' => $table]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.old_users RENAME TO new_users', $sqls[0]->toSql());
    }

    public function test_returns_empty_when_no_changes(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s);

        static::assertSame([], $diff->generate());
    }

    public function test_reversed_added_table(): void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s, removedTables: [schema_table('users', [
            schema_column_integer('id', false),
            schema_column_text('name'),
        ])]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP TABLE public.users CASCADE', $sqls[0]->toSql());
    }

    public function test_reversed_table_rename(): void
    {
        $s = schema('public');
        $table = schema_table('old_users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff($s, $s, renamedTables: ['public.new_users' => $table]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.new_users RENAME TO old_users', $sqls[0]->toSql());
    }
}
