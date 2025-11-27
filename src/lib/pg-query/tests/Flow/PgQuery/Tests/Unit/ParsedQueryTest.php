<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit;

use Flow\PgQuery\AST\Nodes\{Column, FunctionCall, Table};
use Flow\PgQuery\AST\Visitors\{ColumnRefCollector, FuncCallCollector, RangeVarCollector};
use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\ParseResult;
use PHPUnit\Framework\TestCase;

final class ParsedQueryTest extends TestCase
{
    private Parser $parser;

    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }

        $this->parser = new Parser();
    }

    public function test_columns_filtered_by_table() : void
    {
        $result = $this->parser->parse('SELECT u.id, o.order_date FROM users u JOIN orders o ON u.id = o.user_id');

        $userColumns = $result->columns('u');
        $orderColumns = $result->columns('o');

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
        $result = $this->parser->parse('SELECT id, name FROM users');

        $columns = $result->columns();

        self::assertCount(2, $columns);

        $columnNames = \array_map(fn (Column $c) => $c->name(), $columns);
        self::assertContains('id', $columnNames);
        self::assertContains('name', $columnNames);
    }

    public function test_columns_from_where_clause() : void
    {
        $result = $this->parser->parse('SELECT 1 FROM users WHERE active = true AND name LIKE \'%john%\'');

        $columns = $result->columns();

        $columnNames = \array_map(fn (Column $c) => $c->name(), $columns);
        self::assertContains('active', $columnNames);
        self::assertContains('name', $columnNames);
    }

    public function test_columns_with_star() : void
    {
        $result = $this->parser->parse('SELECT * FROM users');

        $columns = $result->columns();

        self::assertCount(1, $columns);
        self::assertSame('*', $columns[0]->name());
        self::assertNull($columns[0]->table());
    }

    public function test_columns_with_table_qualified_star() : void
    {
        $result = $this->parser->parse('SELECT u.* FROM users u');

        $columns = $result->columns();

        self::assertCount(1, $columns);
        self::assertSame('*', $columns[0]->name());
        self::assertSame('u', $columns[0]->table());
    }

    public function test_columns_with_table_qualifier() : void
    {
        $result = $this->parser->parse('SELECT u.id, u.name FROM users u');

        $columns = $result->columns();

        self::assertCount(2, $columns);

        foreach ($columns as $column) {
            self::assertSame('u', $column->table());
        }
    }

    public function test_functions_from_select() : void
    {
        $result = $this->parser->parse('SELECT COUNT(*), SUM(amount) FROM orders');

        $functions = $result->functions();

        self::assertCount(2, $functions);

        $functionNames = \array_map(fn (FunctionCall $f) => $f->name(), $functions);
        self::assertContains('count', $functionNames);
        self::assertContains('sum', $functionNames);
    }

    public function test_functions_nested() : void
    {
        $result = $this->parser->parse('SELECT UPPER(CONCAT(first_name, last_name)) FROM users');

        $functions = $result->functions();

        self::assertCount(2, $functions);

        $functionNames = \array_map(fn (FunctionCall $f) => $f->name(), $functions);
        self::assertContains('upper', $functionNames);
        self::assertContains('concat', $functionNames);
    }

    public function test_functions_with_schema() : void
    {
        $result = $this->parser->parse('SELECT pg_catalog.now()');

        $functions = $result->functions();

        self::assertCount(1, $functions);
        self::assertSame('now', $functions[0]->name());
        self::assertSame('pg_catalog', $functions[0]->schema());
    }

    public function test_raw_returns_parse_result() : void
    {
        $result = $this->parser->parse('SELECT 1');

        self::assertInstanceOf(ParseResult::class, $result->raw());
    }

    public function test_tables_from_cte() : void
    {
        $result = $this->parser->parse('WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users');

        $tables = $result->tables();

        self::assertCount(2, $tables);

        $tableNames = \array_map(fn (Table $t) => $t->name(), $tables);
        self::assertContains('users', $tableNames);
        self::assertContains('active_users', $tableNames);
    }

    public function test_tables_from_delete() : void
    {
        $result = $this->parser->parse('DELETE FROM users WHERE id = 1');

        $tables = $result->tables();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_from_insert() : void
    {
        $result = $this->parser->parse('INSERT INTO users (name) VALUES (\'john\')');

        $tables = $result->tables();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_from_join() : void
    {
        $result = $this->parser->parse('SELECT * FROM users u JOIN orders o ON u.id = o.user_id');

        $tables = $result->tables();

        self::assertCount(2, $tables);

        $tableNames = \array_map(fn (Table $t) => $t->name(), $tables);
        self::assertContains('users', $tableNames);
        self::assertContains('orders', $tableNames);
    }

    public function test_tables_from_simple_select() : void
    {
        $result = $this->parser->parse('SELECT * FROM users');

        $tables = $result->tables();

        self::assertCount(1, $tables);
        self::assertInstanceOf(Table::class, $tables[0]);
        self::assertSame('users', $tables[0]->name());
        self::assertNull($tables[0]->schema());
        self::assertNull($tables[0]->alias());
    }

    public function test_tables_from_subquery() : void
    {
        $result = $this->parser->parse('SELECT * FROM (SELECT * FROM orders) AS sub');

        $tables = $result->tables();

        self::assertCount(1, $tables);
        self::assertSame('orders', $tables[0]->name());
    }

    public function test_tables_from_update() : void
    {
        $result = $this->parser->parse('UPDATE users SET name = \'john\' WHERE id = 1');

        $tables = $result->tables();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_with_alias() : void
    {
        $result = $this->parser->parse('SELECT * FROM users AS u');

        $tables = $result->tables();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
        self::assertSame('u', $tables[0]->alias());
    }

    public function test_tables_with_schema() : void
    {
        $result = $this->parser->parse('SELECT * FROM public.users');

        $tables = $result->tables();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
        self::assertSame('public', $tables[0]->schema());
    }

    public function test_traverse_with_multiple_visitors() : void
    {
        $result = $this->parser->parse('SELECT COUNT(id), name FROM users WHERE active = true');

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
        $result = $this->parser->parse('SELECT id, name FROM users');

        $collector = new ColumnRefCollector();
        $result->traverse($collector);

        self::assertCount(2, $collector->getColumnRefs());
    }
}
