<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit;

use function Flow\PostgreSql\DSL\{sql_parse, sql_query_columns, sql_query_functions, sql_query_tables};
use Flow\PostgreSql\AST\Nodes\{Column, FunctionCall, Statements, Table};
use Flow\PostgreSql\AST\Nodes\Statement\{DeleteStatement, InsertStatement, SelectStatement, UpdateStatement};
use Flow\PostgreSql\AST\Visitors\{ColumnRefCollector, FuncCallCollector, RangeVarCollector};
use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use PHPUnit\Framework\TestCase;

final class ParsedQueryTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_columns_filtered_by_table() : void
    {
        $result = sql_parse('SELECT u.id, o.order_date FROM users u JOIN orders o ON u.id = o.user_id');

        $userColumns = sql_query_columns($result)->forTable('u');
        $orderColumns = sql_query_columns($result)->forTable('o');

        self::assertCount(2, $userColumns);
        $userColumnNames = \array_map(fn (Column $c) => $c->name(), $userColumns);
        self::assertContains('id', $userColumnNames);

        self::assertCount(2, $orderColumns);
        $orderColumnNames = \array_map(fn (Column $c) => $c->name(), $orderColumns);
        self::assertContains('order_date', $orderColumnNames);
        self::assertContains('user_id', $orderColumnNames);
    }

    public function test_columns_from_select() : void
    {
        $result = sql_parse('SELECT id, name FROM users');

        $columns = sql_query_columns($result)->all();

        self::assertCount(2, $columns);

        $columnNames = \array_map(fn (Column $c) => $c->name(), $columns);
        self::assertContains('id', $columnNames);
        self::assertContains('name', $columnNames);
    }

    public function test_columns_from_where_clause() : void
    {
        $result = sql_parse('SELECT 1 FROM users WHERE active = true AND name LIKE \'%john%\'');

        $columns = sql_query_columns($result)->all();

        $columnNames = \array_map(fn (Column $c) => $c->name(), $columns);
        self::assertContains('active', $columnNames);
        self::assertContains('name', $columnNames);
    }

    public function test_columns_with_star() : void
    {
        $result = sql_parse('SELECT * FROM users');

        $columns = sql_query_columns($result)->all();

        self::assertCount(1, $columns);
        self::assertSame('*', $columns[0]->name());
        self::assertNull($columns[0]->table());
    }

    public function test_columns_with_table_qualified_star() : void
    {
        $result = sql_parse('SELECT u.* FROM users u');

        $columns = sql_query_columns($result)->all();

        self::assertCount(1, $columns);
        self::assertSame('*', $columns[0]->name());
        self::assertSame('u', $columns[0]->table());
    }

    public function test_columns_with_table_qualifier() : void
    {
        $result = sql_parse('SELECT u.id, u.name FROM users u');

        $columns = sql_query_columns($result)->all();

        self::assertCount(2, $columns);

        foreach ($columns as $column) {
            self::assertSame('u', $column->table());
        }
    }

    public function test_delete_statement_to_builder() : void
    {
        $result = sql_parse('DELETE FROM users WHERE id = 1');

        $deleteStatement = $result->statements()->first();
        self::assertInstanceOf(DeleteStatement::class, $deleteStatement);
    }

    public function test_functions_from_select() : void
    {
        $result = sql_parse('SELECT COUNT(*), SUM(amount) FROM orders');

        $functions = sql_query_functions($result)->all();

        self::assertCount(2, $functions);

        $functionNames = \array_map(fn (FunctionCall $f) => $f->name(), $functions);
        self::assertContains('count', $functionNames);
        self::assertContains('sum', $functionNames);
    }

    public function test_functions_nested() : void
    {
        $result = sql_parse('SELECT UPPER(CONCAT(first_name, last_name)) FROM users');

        $functions = sql_query_functions($result)->all();

        self::assertCount(2, $functions);

        $functionNames = \array_map(fn (FunctionCall $f) => $f->name(), $functions);
        self::assertContains('upper', $functionNames);
        self::assertContains('concat', $functionNames);
    }

    public function test_functions_with_schema() : void
    {
        $result = sql_parse('SELECT pg_catalog.now()');

        $functions = sql_query_functions($result)->all();

        self::assertCount(1, $functions);
        self::assertSame('now', $functions[0]->name());
        self::assertSame('pg_catalog', $functions[0]->schema());
    }

    public function test_insert_statement_to_builder() : void
    {
        $result = sql_parse("INSERT INTO users (name) VALUES ('John')");

        $insertStatement = $result->statements()->first();
        self::assertInstanceOf(InsertStatement::class, $insertStatement);
    }

    public function test_raw_returns_parse_result() : void
    {
        $result = sql_parse('SELECT 1');

        self::assertInstanceOf(ParseResult::class, $result->raw());
    }

    public function test_select_statement_to_builder() : void
    {
        $result = sql_parse('SELECT * FROM users');

        $selectStatement = $result->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $selectStatement);
    }

    public function test_statements_all() : void
    {
        $result = sql_parse('SELECT 1; SELECT 2');

        $all = $result->statements()->all();

        self::assertCount(2, $all);
        self::assertInstanceOf(SelectStatement::class, $all[0]);
        self::assertInstanceOf(SelectStatement::class, $all[1]);
    }

    public function test_statements_count() : void
    {
        $result = sql_parse('SELECT * FROM users');

        self::assertCount(1, $result->statements());
    }

    public function test_statements_first() : void
    {
        $result = sql_parse('SELECT * FROM users');

        $first = $result->statements()->first();

        self::assertNotNull($first);
        self::assertInstanceOf(SelectStatement::class, $first);
    }

    public function test_statements_first_is_delete_statement() : void
    {
        $result = sql_parse('DELETE FROM users WHERE id = 1');

        self::assertInstanceOf(DeleteStatement::class, $result->statements()->first());
    }

    public function test_statements_first_is_insert_statement() : void
    {
        $result = sql_parse("INSERT INTO users (name) VALUES ('John')");

        self::assertInstanceOf(InsertStatement::class, $result->statements()->first());
    }

    public function test_statements_first_is_select_statement() : void
    {
        $result = sql_parse('SELECT * FROM users');

        self::assertInstanceOf(SelectStatement::class, $result->statements()->first());
    }

    public function test_statements_first_is_update_statement() : void
    {
        $result = sql_parse("UPDATE users SET name = 'John' WHERE id = 1");

        self::assertInstanceOf(UpdateStatement::class, $result->statements()->first());
    }

    public function test_statements_get() : void
    {
        $result = sql_parse('SELECT 1; INSERT INTO users (id) VALUES (1)');

        self::assertInstanceOf(SelectStatement::class, $result->statements()->get(0));
        self::assertInstanceOf(InsertStatement::class, $result->statements()->get(1));
        self::assertNull($result->statements()->get(2));
    }

    public function test_statements_is_empty() : void
    {
        $sql = new ParseResult();
        $sql->setStmts([]);
        $result = new ParsedQuery($sql);

        self::assertTrue($result->statements()->isEmpty());
    }

    public function test_statements_is_single() : void
    {
        $result = sql_parse('SELECT * FROM users');

        self::assertTrue($result->statements()->isSingle());
    }

    public function test_statements_iterable() : void
    {
        $result = sql_parse('SELECT 1; SELECT 2');

        $count = 0;

        foreach ($result->statements() as $statement) {
            self::assertInstanceOf(SelectStatement::class, $statement);
            $count++;
        }

        self::assertSame(2, $count);
    }

    public function test_statements_last() : void
    {
        $result = sql_parse('SELECT 1; INSERT INTO users (id) VALUES (1)');

        $last = $result->statements()->last();

        self::assertNotNull($last);
        self::assertInstanceOf(InsertStatement::class, $last);
    }

    public function test_statements_returns_statements_collection() : void
    {
        $result = sql_parse('SELECT * FROM users');

        $statements = $result->statements();

        self::assertInstanceOf(Statements::class, $statements);
    }

    public function test_statements_with_multiple_statements() : void
    {
        $result = sql_parse('SELECT 1; SELECT 2');

        self::assertCount(2, $result->statements());
        self::assertFalse($result->statements()->isSingle());
    }

    public function test_tables_from_cte() : void
    {
        $result = sql_parse('WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users');

        $tables = sql_query_tables($result)->all();

        self::assertCount(2, $tables);

        $tableNames = \array_map(fn (Table $t) => $t->name(), $tables);
        self::assertContains('users', $tableNames);
        self::assertContains('active_users', $tableNames);
    }

    public function test_tables_from_delete() : void
    {
        $result = sql_parse('DELETE FROM users WHERE id = 1');

        $tables = sql_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_from_insert() : void
    {
        $result = sql_parse('INSERT INTO users (name) VALUES (\'john\')');

        $tables = sql_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_from_join() : void
    {
        $result = sql_parse('SELECT * FROM users u JOIN orders o ON u.id = o.user_id');

        $tables = sql_query_tables($result)->all();

        self::assertCount(2, $tables);

        $tableNames = \array_map(fn (Table $t) => $t->name(), $tables);
        self::assertContains('users', $tableNames);
        self::assertContains('orders', $tableNames);
    }

    public function test_tables_from_simple_select() : void
    {
        $result = sql_parse('SELECT * FROM users');

        $tables = sql_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertInstanceOf(Table::class, $tables[0]);
        self::assertSame('users', $tables[0]->name());
        self::assertNull($tables[0]->schema());
        self::assertNull($tables[0]->alias());
    }

    public function test_tables_from_subquery() : void
    {
        $result = sql_parse('SELECT * FROM (SELECT * FROM orders) AS sub');

        $tables = sql_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('orders', $tables[0]->name());
    }

    public function test_tables_from_update() : void
    {
        $result = sql_parse('UPDATE users SET name = \'john\' WHERE id = 1');

        $tables = sql_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_with_alias() : void
    {
        $result = sql_parse('SELECT * FROM users AS u');

        $tables = sql_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
        self::assertSame('u', $tables[0]->alias());
    }

    public function test_tables_with_schema() : void
    {
        $result = sql_parse('SELECT * FROM public.users');

        $tables = sql_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
        self::assertSame('public', $tables[0]->schema());
    }

    public function test_traverse_with_multiple_visitors() : void
    {
        $result = sql_parse('SELECT COUNT(id), name FROM users WHERE active = true');

        $columnCollector = new ColumnRefCollector();
        $funcCollector = new FuncCallCollector();
        $rangeVarCollector = new RangeVarCollector();

        $result->traverse($columnCollector, $funcCollector, $rangeVarCollector);

        self::assertCount(3, $columnCollector->getColumnRefs());
        self::assertCount(1, $funcCollector->getFuncCalls());
        self::assertCount(1, $rangeVarCollector->getRangeVars());
    }

    public function test_traverse_with_single_visitor() : void
    {
        $result = sql_parse('SELECT id, name FROM users');

        $collector = new ColumnRefCollector();
        $result->traverse($collector);

        self::assertCount(2, $collector->getColumnRefs());
    }

    public function test_update_statement_to_builder() : void
    {
        $result = sql_parse("UPDATE users SET name = 'John' WHERE id = 1");

        $updateStatement = $result->statements()->first();
        self::assertInstanceOf(UpdateStatement::class, $updateStatement);
    }
}
