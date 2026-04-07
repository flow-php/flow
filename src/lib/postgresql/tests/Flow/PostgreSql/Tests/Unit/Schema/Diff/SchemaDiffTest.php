<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\{literal, schema, schema_column_integer, schema_column_text, schema_domain, schema_extension, schema_function, schema_materialized_view, schema_procedure, schema_sequence, schema_table, schema_view, select};

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\{Column, Domain};
use Flow\PostgreSql\Schema\Diff\{SchemaDiff, SequenceDiff, TableDiff};
use PHPUnit\Framework\TestCase;

final class SchemaDiffTest extends TestCase
{
    public function test_creates_added_domain() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedDomains: [schema_domain('email', ColumnType::text(), nullable: false)],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE DOMAIN email AS pg_catalog.text NOT NULL', $sqls[0]->toSql());
    }

    public function test_creates_added_extension() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedExtensions: [schema_extension('uuid-ossp')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE EXTENSION "uuid-ossp"', $sqls[0]->toSql());
    }

    public function test_creates_added_function() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedFunctions: [schema_function('my_func', 'text', ['integer'], 'sql', 'SELECT name FROM users WHERE id = $1')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE OR REPLACE FUNCTION my_func(IN int) RETURNS text LANGUAGE sql AS $$SELECT name FROM users WHERE id = $1$$', $sqls[0]->toSql());
    }

    public function test_creates_added_sequence() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedSequences: [schema_sequence('users_id_seq')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE SEQUENCE users_id_seq AS bigint START 1 INCREMENT 1 MINVALUE 1 CACHE 1 NO MAXVALUE', $sqls[0]->toSql());
    }

    public function test_creates_added_table() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            [schema_table('users', [schema_column_integer('id', false), schema_column_text('name')])],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE TABLE public.users (id int NOT NULL, name pg_catalog.text)', $sqls[0]->toSql());
    }

    public function test_creates_added_view() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedViews: [schema_view('active_users', 'SELECT * FROM users WHERE active = true')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE VIEW active_users AS SELECT * FROM users WHERE active = true', $sqls[0]->toSql());
    }

    public function test_delegates_to_table_diff() : void
    {
        $s = schema('public');
        $sourceTable = schema_table('users', [schema_column_integer('id', false)]);
        $targetTable = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);

        $tableDiff = new TableDiff(
            $sourceTable,
            $targetTable,
            [new Column('name', ColumnType::text(), true)],
        );

        $diff = new SchemaDiff(
            $s,
            $s,
            modifiedTables: [$tableDiff],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER TABLE public.users ADD COLUMN name pg_catalog.text', $sqls[0]->toSql());
    }

    public function test_dependency_order() : void
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

        self::assertCount(3, $sqls);
        self::assertSame('CREATE EXTENSION "uuid-ossp"', $sqls[0]->toSql());
        self::assertSame('CREATE TABLE public.users (id int NOT NULL)', $sqls[1]->toSql());
        self::assertSame('DROP VIEW active_users', $sqls[2]->toSql());
    }

    public function test_drops_all_removed_object_types() : void
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

        self::assertSame('DROP MATERIALIZED VIEW mv_stats', $sqls[0]->toSql());
        self::assertSame('DROP VIEW active_users', $sqls[1]->toSql());
        self::assertSame('DROP TABLE public.users CASCADE', $sqls[2]->toSql());
        self::assertSame('DROP PROCEDURE cleanup', $sqls[3]->toSql());
        self::assertSame('DROP FUNCTION my_func', $sqls[4]->toSql());
        self::assertSame('DROP SEQUENCE users_id_seq', $sqls[5]->toSql());
        self::assertSame('DROP DOMAIN email CASCADE', $sqls[6]->toSql());
        self::assertSame('DROP EXTENSION pgcrypto', $sqls[7]->toSql());
        self::assertCount(8, $sqls);
    }

    public function test_drops_removed_domain() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedDomains: [schema_domain('email', ColumnType::text())],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP DOMAIN email CASCADE', $sqls[0]->toSql());
    }

    public function test_drops_removed_extension() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedExtensions: [schema_extension('uuid-ossp')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP EXTENSION "uuid-ossp"', $sqls[0]->toSql());
    }

    public function test_drops_removed_function() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedFunctions: [schema_function('my_func', 'text')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP FUNCTION my_func', $sqls[0]->toSql());
    }

    public function test_drops_removed_materialized_view() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedMaterializedViews: [schema_materialized_view('mv_stats', select(literal(1))->toSql())],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP MATERIALIZED VIEW mv_stats', $sqls[0]->toSql());
    }

    public function test_drops_removed_procedure() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedProcedures: [schema_procedure('cleanup')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP PROCEDURE cleanup', $sqls[0]->toSql());
    }

    public function test_drops_removed_sequence() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedSequences: [schema_sequence('users_id_seq')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP SEQUENCE users_id_seq', $sqls[0]->toSql());
    }

    public function test_drops_removed_table() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedTables: [schema_table('users', [schema_column_integer('id', false)])],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP TABLE public.users CASCADE', $sqls[0]->toSql());
    }

    public function test_drops_removed_view() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedViews: [schema_view('active_users', 'SELECT * FROM users WHERE active = true')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP VIEW active_users', $sqls[0]->toSql());
    }

    public function test_is_empty_when_all_arrays_empty() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s);

        self::assertTrue($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_domains() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedDomains: [new Domain('email', ColumnType::text())],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_extensions() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedExtensions: [schema_extension('uuid-ossp')],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_functions() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedFunctions: [schema_function('f', 'void')],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_procedures() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedProcedures: [schema_procedure('p')],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_tables() : void
    {
        $s = schema('public');
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff(
            $s,
            $s,
            [$table],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_added_views() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            addedViews: [schema_view('v', select(literal(1))->toSql())],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_modified_sequences() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            modifiedSequences: [new SequenceDiff(schema_sequence('s1'), schema_sequence('s1', incrementBy: 5))],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_removed_extensions() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedExtensions: [schema_extension('uuid-ossp')],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_removed_materialized_views() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedMaterializedViews: [schema_materialized_view('mv', select(literal(1))->toSql())],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_removed_tables() : void
    {
        $s = schema('public');
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff(
            $s,
            $s,
            removedTables: [$table],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_is_not_empty_when_renamed_tables() : void
    {
        $s = schema('public');
        $table = schema_table('new_users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff(
            $s,
            $s,
            renamedTables: ['public.old_users' => $table],
        );

        self::assertFalse($diff->isEmpty());
    }

    public function test_renames_table() : void
    {
        $s = schema('public');
        $table = schema_table('new_users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff(
            $s,
            $s,
            renamedTables: ['public.old_users' => $table],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER TABLE public.old_users RENAME TO new_users', $sqls[0]->toSql());
    }

    public function test_returns_empty_when_no_changes() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff($s, $s);

        self::assertSame([], $diff->generate());
    }

    public function test_reversed_added_table() : void
    {
        $s = schema('public');

        $diff = new SchemaDiff(
            $s,
            $s,
            removedTables: [schema_table('users', [schema_column_integer('id', false), schema_column_text('name')])],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP TABLE public.users CASCADE', $sqls[0]->toSql());
    }

    public function test_reversed_table_rename() : void
    {
        $s = schema('public');
        $table = schema_table('old_users', [schema_column_integer('id', false)]);

        $diff = new SchemaDiff(
            $s,
            $s,
            renamedTables: ['public.new_users' => $table],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER TABLE public.new_users RENAME TO old_users', $sqls[0]->toSql());
    }
}
