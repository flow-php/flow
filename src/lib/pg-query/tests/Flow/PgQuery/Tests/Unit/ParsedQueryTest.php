<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit;

use function Flow\PgQuery\DSL\{pg_and, pg_col, pg_eq, pg_gt, pg_int, pg_parse, pg_query_columns, pg_query_functions, pg_query_tables, pg_to_query_builder};
use Flow\PgQuery\AST\Nodes\{Column, FunctionCall, Table};
use Flow\PgQuery\AST\Visitors\{ColumnRefCollector, FuncCallCollector, RangeVarCollector};
use Flow\PgQuery\ParsedQuery;
use Flow\PgQuery\Protobuf\AST\{Node, ParseResult, RawStmt};
use Flow\PgQuery\QueryBuilder\Delete\DeleteBuilder;
use Flow\PgQuery\QueryBuilder\Insert\InsertBuilder;
use Flow\PgQuery\QueryBuilder\Select\SelectBuilder;
use Flow\PgQuery\QueryBuilder\Update\UpdateBuilder;
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
        $result = pg_parse('SELECT u.id, o.order_date FROM users u JOIN orders o ON u.id = o.user_id');

        $userColumns = pg_query_columns($result)->forTable('u');
        $orderColumns = pg_query_columns($result)->forTable('o');

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
        $result = pg_parse('SELECT id, name FROM users');

        $columns = pg_query_columns($result)->all();

        self::assertCount(2, $columns);

        $columnNames = \array_map(fn (Column $c) => $c->name(), $columns);
        self::assertContains('id', $columnNames);
        self::assertContains('name', $columnNames);
    }

    public function test_columns_from_where_clause() : void
    {
        $result = pg_parse('SELECT 1 FROM users WHERE active = true AND name LIKE \'%john%\'');

        $columns = pg_query_columns($result)->all();

        $columnNames = \array_map(fn (Column $c) => $c->name(), $columns);
        self::assertContains('active', $columnNames);
        self::assertContains('name', $columnNames);
    }

    public function test_columns_with_star() : void
    {
        $result = pg_parse('SELECT * FROM users');

        $columns = pg_query_columns($result)->all();

        self::assertCount(1, $columns);
        self::assertSame('*', $columns[0]->name());
        self::assertNull($columns[0]->table());
    }

    public function test_columns_with_table_qualified_star() : void
    {
        $result = pg_parse('SELECT u.* FROM users u');

        $columns = pg_query_columns($result)->all();

        self::assertCount(1, $columns);
        self::assertSame('*', $columns[0]->name());
        self::assertSame('u', $columns[0]->table());
    }

    public function test_columns_with_table_qualifier() : void
    {
        $result = pg_parse('SELECT u.id, u.name FROM users u');

        $columns = pg_query_columns($result)->all();

        self::assertCount(2, $columns);

        foreach ($columns as $column) {
            self::assertSame('u', $column->table());
        }
    }

    public function test_functions_from_select() : void
    {
        $result = pg_parse('SELECT COUNT(*), SUM(amount) FROM orders');

        $functions = pg_query_functions($result)->all();

        self::assertCount(2, $functions);

        $functionNames = \array_map(fn (FunctionCall $f) => $f->name(), $functions);
        self::assertContains('count', $functionNames);
        self::assertContains('sum', $functionNames);
    }

    public function test_functions_nested() : void
    {
        $result = pg_parse('SELECT UPPER(CONCAT(first_name, last_name)) FROM users');

        $functions = pg_query_functions($result)->all();

        self::assertCount(2, $functions);

        $functionNames = \array_map(fn (FunctionCall $f) => $f->name(), $functions);
        self::assertContains('upper', $functionNames);
        self::assertContains('concat', $functionNames);
    }

    public function test_functions_with_schema() : void
    {
        $result = pg_parse('SELECT pg_catalog.now()');

        $functions = pg_query_functions($result)->all();

        self::assertCount(1, $functions);
        self::assertSame('now', $functions[0]->name());
        self::assertSame('pg_catalog', $functions[0]->schema());
    }

    public function test_raw_returns_parse_result() : void
    {
        $result = pg_parse('SELECT 1');

        self::assertInstanceOf(ParseResult::class, $result->raw());
    }

    public function test_tables_from_cte() : void
    {
        $result = pg_parse('WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users');

        $tables = pg_query_tables($result)->all();

        self::assertCount(2, $tables);

        $tableNames = \array_map(fn (Table $t) => $t->name(), $tables);
        self::assertContains('users', $tableNames);
        self::assertContains('active_users', $tableNames);
    }

    public function test_tables_from_delete() : void
    {
        $result = pg_parse('DELETE FROM users WHERE id = 1');

        $tables = pg_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_from_insert() : void
    {
        $result = pg_parse('INSERT INTO users (name) VALUES (\'john\')');

        $tables = pg_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_from_join() : void
    {
        $result = pg_parse('SELECT * FROM users u JOIN orders o ON u.id = o.user_id');

        $tables = pg_query_tables($result)->all();

        self::assertCount(2, $tables);

        $tableNames = \array_map(fn (Table $t) => $t->name(), $tables);
        self::assertContains('users', $tableNames);
        self::assertContains('orders', $tableNames);
    }

    public function test_tables_from_simple_select() : void
    {
        $result = pg_parse('SELECT * FROM users');

        $tables = pg_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertInstanceOf(Table::class, $tables[0]);
        self::assertSame('users', $tables[0]->name());
        self::assertNull($tables[0]->schema());
        self::assertNull($tables[0]->alias());
    }

    public function test_tables_from_subquery() : void
    {
        $result = pg_parse('SELECT * FROM (SELECT * FROM orders) AS sub');

        $tables = pg_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('orders', $tables[0]->name());
    }

    public function test_tables_from_update() : void
    {
        $result = pg_parse('UPDATE users SET name = \'john\' WHERE id = 1');

        $tables = pg_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
    }

    public function test_tables_with_alias() : void
    {
        $result = pg_parse('SELECT * FROM users AS u');

        $tables = pg_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
        self::assertSame('u', $tables[0]->alias());
    }

    public function test_tables_with_schema() : void
    {
        $result = pg_parse('SELECT * FROM public.users');

        $tables = pg_query_tables($result)->all();

        self::assertCount(1, $tables);
        self::assertSame('users', $tables[0]->name());
        self::assertSame('public', $tables[0]->schema());
    }

    public function test_to_delete_builder() : void
    {
        $builder = pg_parse('DELETE FROM users WHERE id = 1')->toDeleteBuilder();

        self::assertInstanceOf(DeleteBuilder::class, $builder);
    }

    public function test_to_delete_builder_throws_on_wrong_type() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Query is not a DELETE statement');

        pg_parse('SELECT * FROM users')->toDeleteBuilder();
    }

    public function test_to_insert_builder() : void
    {
        $builder = pg_parse("INSERT INTO users (name) VALUES ('John')")->toInsertBuilder();

        self::assertInstanceOf(InsertBuilder::class, $builder);
    }

    public function test_to_insert_builder_throws_on_wrong_type() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Query is not an INSERT statement');

        pg_parse('SELECT * FROM users')->toInsertBuilder();
    }

    public function test_to_query_builder_delete() : void
    {
        $builder = pg_parse('DELETE FROM users WHERE id = 1')->toQueryBuilder();

        self::assertInstanceOf(DeleteBuilder::class, $builder);
    }

    public function test_to_query_builder_dsl_function() : void
    {
        $builder = pg_to_query_builder('SELECT * FROM users');

        self::assertInstanceOf(SelectBuilder::class, $builder);
    }

    public function test_to_query_builder_insert() : void
    {
        $builder = pg_parse("INSERT INTO users (name) VALUES ('John')")->toQueryBuilder();

        self::assertInstanceOf(InsertBuilder::class, $builder);
    }

    public function test_to_query_builder_modify_and_deparse() : void
    {
        $builder = pg_parse('SELECT * FROM users')->toQueryBuilder();

        self::assertInstanceOf(SelectBuilder::class, $builder);

        $modified = $builder
            ->where(pg_and(
                pg_eq(pg_col('id'), pg_int(1)),
                pg_gt(pg_col('age'), pg_int(18))
            ))
            ->limit(10);

        $sql = pg_parse('SELECT 1')->raw();
        $rawStmt = new RawStmt();
        $node = new Node();
        $node->setSelectStmt($modified->toAst());
        $rawStmt->setStmt($node);
        $sql->setStmts([$rawStmt]);

        $result = (new ParsedQuery($sql))->deparse();

        self::assertSame('SELECT * FROM users WHERE id = 1 AND age > 18 LIMIT 10', $result);
    }

    public function test_to_query_builder_select() : void
    {
        $builder = pg_parse('SELECT * FROM users')->toQueryBuilder();

        self::assertInstanceOf(SelectBuilder::class, $builder);
    }

    public function test_to_query_builder_throws_on_multiple_statements() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Multiple statements found. Use pg_split() to parse statements individually.');

        pg_parse('SELECT 1; SELECT 2')->toQueryBuilder();
    }

    public function test_to_query_builder_update() : void
    {
        $builder = pg_parse("UPDATE users SET name = 'John' WHERE id = 1")->toQueryBuilder();

        self::assertInstanceOf(UpdateBuilder::class, $builder);
    }

    public function test_to_select_builder() : void
    {
        $builder = pg_parse('SELECT * FROM users')->toSelectBuilder();

        self::assertInstanceOf(SelectBuilder::class, $builder);
    }

    public function test_to_select_builder_throws_on_wrong_type() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Query is not a SELECT statement');

        pg_parse('DELETE FROM users')->toSelectBuilder();
    }

    public function test_to_update_builder() : void
    {
        $builder = pg_parse("UPDATE users SET name = 'John' WHERE id = 1")->toUpdateBuilder();

        self::assertInstanceOf(UpdateBuilder::class, $builder);
    }

    public function test_to_update_builder_throws_on_wrong_type() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Query is not an UPDATE statement');

        pg_parse('SELECT * FROM users')->toUpdateBuilder();
    }

    public function test_traverse_with_multiple_visitors() : void
    {
        $result = pg_parse('SELECT COUNT(id), name FROM users WHERE active = true');

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
        $result = pg_parse('SELECT id, name FROM users');

        $collector = new ColumnRefCollector();
        $result->traverse($collector);

        self::assertCount(2, $collector->getColumnRefs());
    }
}
