<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Delete;

use Flow\PostgreSql\{ParsedQuery, Parser};
use Flow\PostgreSql\Protobuf\AST\{DeleteStmt, Node, RawStmt, SelectStmt};
use Flow\PostgreSql\QueryBuilder\Clause\{CTE, WithClause};
use Flow\PostgreSql\QueryBuilder\Condition\{Comparison, ComparisonOperator};
use Flow\PostgreSql\QueryBuilder\Delete\{DeleteBuilder, DeleteFinalStep};
use Flow\PostgreSql\QueryBuilder\Expression\{Column, FunctionCall, Literal};
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class DeleteBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_builder_steps_allow_fluent_interface() : void
    {
        $query = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'active'), ComparisonOperator::EQ, Literal::bool(false)))
            ->returning(Column::name('id'));

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);
    }

    public function test_delete_can_skip_optional_steps() : void
    {
        $queryWithoutWhere = DeleteBuilder::create()
            ->from('temp_data')
            ->returningAll();

        $ast = $queryWithoutWhere->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);
        self::assertFalse($ast->hasWhereClause());

        $returningList = $ast->getReturningList();
        self::assertCount(1, $returningList);
    }

    public function test_delete_complex_query() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $cte = new CTE('cancelled_orders', $selectNode);
        $withClause = new WithClause([$cte]);

        $query = DeleteBuilder::with($withClause)
            ->from('order_items', 'oi')
            ->using((new Table('cancelled_orders'))->as('co'))
            ->where(
                new Comparison(
                    Column::tableColumn('oi', 'order_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('co', 'id')
                )
            )
            ->returning(
                Column::tableColumn('oi', 'id'),
                Column::tableColumn('oi', 'product_id'),
                Column::tableColumn('oi', 'quantity')
            );

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);
        self::assertTrue($ast->hasWithClause());

        $usingClause = $ast->getUsingClause();
        self::assertNotNull($usingClause);
        self::assertCount(1, $usingClause);

        self::assertTrue($ast->hasWhereClause());

        $returningList = $ast->getReturningList();
        self::assertNotNull($returningList);
        self::assertCount(3, $returningList);
    }

    public function test_delete_only_from_is_valid() : void
    {
        $query = DeleteBuilder::create()
            ->from('all_data');

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('all_data', $relation->getRelname());
        self::assertFalse($ast->hasWhereClause());
        self::assertCount(0, $ast->getReturningList());
        self::assertCount(0, $ast->getUsingClause());
    }

    public function test_delete_returning_all() : void
    {
        $query = DeleteBuilder::create()
            ->from('temp_data')
            ->returningAll();

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $returningList = $ast->getReturningList();
        self::assertNotNull($returningList);
        self::assertCount(1, $returningList);

        $firstReturn = $returningList[0]->getResTarget();
        self::assertNotNull($firstReturn);

        $val = $firstReturn->getVal();
        self::assertNotNull($val);

        self::assertTrue($val->hasColumnRef());
        $columnRef = $val->getColumnRef();
        self::assertNotNull($columnRef);
        $fields = $columnRef->getFields();
        self::assertNotNull($fields);
        self::assertCount(1, $fields);
        self::assertTrue($fields[0]->hasAStar());
    }

    public function test_delete_with_alias() : void
    {
        $query = DeleteBuilder::create()
            ->from('users', 'u');

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());

        $alias = $relation->getAlias();
        self::assertNotNull($alias);
        self::assertSame('u', $alias->getAliasname());
    }

    public function test_delete_with_alias_and_where() : void
    {
        $query = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Literal::int(1)));

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());

        $alias = $relation->getAlias();
        self::assertNotNull($alias);
        self::assertSame('u', $alias->getAliasname());
        self::assertTrue($ast->hasWhereClause());
    }

    public function test_delete_with_alias_and_where_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'active'), ComparisonOperator::EQ, Literal::bool(false)));

        $deparsed = $this->deparse($delete->toAst());
        self::assertSame('DELETE FROM users u WHERE u.active = false', $deparsed);
    }

    public function test_delete_with_multiple_using_tables() : void
    {
        $query = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using(
                (new Table('orders'))->as('o'),
                (new Table('customers'))->as('c')
            )
            ->where(
                (new Comparison(Column::tableColumn('oi', 'order_id'), ComparisonOperator::EQ, Column::tableColumn('o', 'id')))
                    ->and(new Comparison(Column::tableColumn('o', 'customer_id'), ComparisonOperator::EQ, Column::tableColumn('c', 'id')))
                    ->and(new Comparison(Column::tableColumn('c', 'status'), ComparisonOperator::EQ, Literal::string('inactive')))
            );

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $usingClause = $ast->getUsingClause();
        self::assertNotNull($usingClause);
        self::assertCount(2, $usingClause);

        $firstTable = $usingClause[0]->getRangeVar();
        self::assertNotNull($firstTable);
        self::assertSame('orders', $firstTable->getRelname());

        $firstAlias = $firstTable->getAlias();
        self::assertNotNull($firstAlias);
        self::assertSame('o', $firstAlias->getAliasname());

        $secondTable = $usingClause[1]->getRangeVar();
        self::assertNotNull($secondTable);
        self::assertSame('customers', $secondTable->getRelname());

        $secondAlias = $secondTable->getAlias();
        self::assertNotNull($secondAlias);
        self::assertSame('c', $secondAlias->getAliasname());
    }

    public function test_delete_with_multiple_using_tables_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using(
                (new Table('orders'))->as('o'),
                (new Table('customers'))->as('c')
            )
            ->where(
                (new Comparison(Column::tableColumn('oi', 'order_id'), ComparisonOperator::EQ, Column::tableColumn('o', 'id')))
                    ->and(new Comparison(Column::tableColumn('o', 'customer_id'), ComparisonOperator::EQ, Column::tableColumn('c', 'id')))
            );

        $deparsed = $this->deparse($delete->toAst());
        self::assertSame('DELETE FROM order_items oi USING orders o, customers c WHERE oi.order_id = o.id AND o.customer_id = c.id', $deparsed);
    }

    public function test_delete_with_returning() : void
    {
        $query = DeleteBuilder::create()
            ->from('sessions')
            ->where(new Comparison(Column::name('expires_at'), ComparisonOperator::LT, new FunctionCall(['now'])))
            ->returning(Column::name('id'), Column::name('user_id'));

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $returningList = $ast->getReturningList();
        self::assertNotNull($returningList);
        self::assertCount(2, $returningList);

        $firstReturn = $returningList[0]->getResTarget();
        self::assertNotNull($firstReturn);

        $firstVal = $firstReturn->getVal();
        self::assertNotNull($firstVal);
        self::assertTrue($firstVal->hasColumnRef());

        $secondReturn = $returningList[1]->getResTarget();
        self::assertNotNull($secondReturn);

        $secondVal = $secondReturn->getVal();
        self::assertNotNull($secondVal);
        self::assertTrue($secondVal->hasColumnRef());
    }

    public function test_delete_with_returning_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('sessions')
            ->where(new Comparison(Column::name('expired'), ComparisonOperator::EQ, Literal::bool(true)))
            ->returning(Column::name('id'));

        $deparsed = $this->deparse($delete->toAst());
        self::assertSame('DELETE FROM sessions WHERE expired = true RETURNING id', $deparsed);
    }

    public function test_delete_with_schema() : void
    {
        $query = DeleteBuilder::create()
            ->from('myschema.users');

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertSame('myschema', $relation->getSchemaname());
    }

    public function test_delete_with_schema_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available.');
        }

        $query = DeleteBuilder::create()
            ->from('public.users');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('DELETE FROM public.users', $deparsed);
    }

    public function test_delete_with_schema_round_trip() : void
    {
        $original = DeleteBuilder::create()
            ->from('myschema.users');

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        self::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        self::assertNotNull($restoredRelation);

        self::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());
        self::assertSame($originalRelation->getSchemaname(), $restoredRelation->getSchemaname());
    }

    public function test_delete_with_using() : void
    {
        $query = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using(new Table('orders'))
            ->where(
                (new Comparison(Column::tableColumn('oi', 'order_id'), ComparisonOperator::EQ, Column::tableColumn('orders', 'id')))
            );

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('order_items', $relation->getRelname());

        $alias = $relation->getAlias();
        self::assertNotNull($alias);
        self::assertSame('oi', $alias->getAliasname());

        $usingClause = $ast->getUsingClause();
        self::assertNotNull($usingClause);
        self::assertCount(1, $usingClause);

        $usingTable = $usingClause[0]->getRangeVar();
        self::assertNotNull($usingTable);
        self::assertSame('orders', $usingTable->getRelname());
    }

    public function test_delete_with_using_and_aliased_table() : void
    {
        $query = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using((new Table('orders'))->as('o'))
            ->where(
                (new Comparison(Column::tableColumn('oi', 'order_id'), ComparisonOperator::EQ, Column::tableColumn('o', 'id')))
                    ->and(new Comparison(Column::tableColumn('o', 'status'), ComparisonOperator::EQ, Literal::string('cancelled')))
            );

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $usingClause = $ast->getUsingClause();
        self::assertNotNull($usingClause);
        self::assertCount(1, $usingClause);

        $usingTable = $usingClause[0]->getRangeVar();
        self::assertNotNull($usingTable);
        self::assertSame('orders', $usingTable->getRelname());
        self::assertNotNull($usingTable->getAlias());
        self::assertSame('o', $usingTable->getAlias()->getAliasname());
    }

    public function test_delete_with_using_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using((new Table('orders'))->as('o'))
            ->where(
                (new Comparison(Column::tableColumn('oi', 'order_id'), ComparisonOperator::EQ, Column::tableColumn('o', 'id')))
                    ->and(new Comparison(Column::tableColumn('o', 'status'), ComparisonOperator::EQ, Literal::string('cancelled')))
            );

        $deparsed = $this->deparse($delete->toAst());
        self::assertSame("DELETE FROM order_items oi USING orders o WHERE oi.order_id = o.id AND o.status = 'cancelled'", $deparsed);
    }

    public function test_delete_with_where() : void
    {
        $query = DeleteBuilder::create()
            ->from('users')
            ->where(new Comparison(Column::name('active'), ComparisonOperator::EQ, Literal::bool(false)));

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);
        self::assertTrue($ast->hasWhereClause());
        self::assertNotNull($ast->getWhereClause());
    }

    public function test_delete_with_where_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('users')
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        $deparsed = $this->deparse($delete->toAst());
        self::assertSame('DELETE FROM users WHERE id = 1', $deparsed);
    }

    public function test_delete_with_with_clause() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $cte = new CTE('inactive_users', $selectNode);
        $withClause = new WithClause([$cte]);

        $query = DeleteBuilder::with($withClause)
            ->from('user_sessions', 'us')
            ->where(
                new Comparison(
                    Column::tableColumn('us', 'user_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('inactive_users', 'id')
                )
            );

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);
        self::assertTrue($ast->hasWithClause());

        $withClauseProto = $ast->getWithClause();
        self::assertNotNull($withClauseProto);

        $ctes = $withClauseProto->getCtes();
        self::assertNotNull($ctes);
        self::assertCount(1, $ctes);

        $firstCte = $ctes[0]->getCommonTableExpr();
        self::assertNotNull($firstCte);
        self::assertSame('inactive_users', $firstCte->getCtename());
    }

    public function test_delete_without_where_returning_all() : void
    {
        $query = DeleteBuilder::create()
            ->from('temp_logs')
            ->returningAll();

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('temp_logs', $relation->getRelname());
        self::assertFalse($ast->hasWhereClause());

        $returningList = $ast->getReturningList();
        self::assertNotNull($returningList);
        self::assertCount(1, $returningList);
    }

    public function test_from_ast_empty_relname_throws_exception() : void
    {
        $this->expectException(\Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "relname" in RangeVar node');

        $deleteStmt = new DeleteStmt();
        $deleteStmt->setRelation(new \Flow\PostgreSql\Protobuf\AST\RangeVar([
            'relname' => '',
        ]));

        DeleteBuilder::fromAst($deleteStmt);
    }

    public function test_from_ast_missing_relation_throws_exception() : void
    {
        $this->expectException(\Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "relation" in DeleteStmt node');

        $deleteStmt = new DeleteStmt();
        DeleteBuilder::fromAst($deleteStmt);
    }

    public function test_immutability_from() : void
    {
        $original = DeleteBuilder::create();
        $modified = $original->from('users');

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_returning() : void
    {
        $original = DeleteBuilder::create()->from('users');
        $modified = $original->returning(Column::name('id'));

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_returning_all() : void
    {
        $original = DeleteBuilder::create()->from('users');
        $modified = $original->returningAll();

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_using() : void
    {
        $original = DeleteBuilder::create()->from('users');
        $modified = $original->using(new Table('orders'));

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_where() : void
    {
        $original = DeleteBuilder::create()->from('users');
        $modified = $original->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        self::assertNotSame($original, $modified);
    }

    public function test_round_trip_complex() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $cte = new CTE('cancelled_orders', $selectNode);
        $withClause = new WithClause([$cte]);

        $original = DeleteBuilder::with($withClause)
            ->from('order_items', 'oi')
            ->using((new Table('cancelled_orders'))->as('co'))
            ->where(
                new Comparison(
                    Column::tableColumn('oi', 'order_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('co', 'id')
                )
            )
            ->returning(Column::name('id'), Column::name('product_id'));

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        self::assertTrue($restoredAst->hasWithClause());

        $withClauseProto = $restoredAst->getWithClause();
        self::assertNotNull($withClauseProto);

        $ctes = $withClauseProto->getCtes();
        self::assertNotNull($ctes);
        self::assertCount(1, $ctes);

        $usingClause = $restoredAst->getUsingClause();
        self::assertCount(1, $usingClause);

        $returningList = $restoredAst->getReturningList();
        self::assertCount(2, $returningList);
    }

    public function test_round_trip_simple() : void
    {
        $original = DeleteBuilder::create()
            ->from('users');

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        self::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        self::assertNotNull($restoredRelation);

        self::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());
        self::assertSame($ast->hasWhereClause(), $restoredAst->hasWhereClause());
    }

    public function test_round_trip_with_alias() : void
    {
        $original = DeleteBuilder::create()
            ->from('users', 'u');

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        self::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        self::assertNotNull($restoredRelation);

        self::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());

        $originalAlias = $originalRelation->getAlias();
        self::assertNotNull($originalAlias);

        $restoredAlias = $restoredRelation->getAlias();
        self::assertNotNull($restoredAlias);

        self::assertSame($originalAlias->getAliasname(), $restoredAlias->getAliasname());
    }

    public function test_round_trip_with_returning() : void
    {
        $original = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Literal::int(1)))
            ->returning(Column::name('id'));

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();
        self::assertTrue($restoredAst->hasWhereClause());

        $returningList = $restoredAst->getReturningList();
        self::assertNotNull($returningList);
        self::assertCount(1, $returningList);
    }

    public function test_round_trip_with_using() : void
    {
        $original = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using((new Table('orders'))->as('o'))
            ->where(
                new Comparison(
                    Column::tableColumn('oi', 'order_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('o', 'id')
                )
            );

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $usingClause = $restoredAst->getUsingClause();
        self::assertNotNull($usingClause);
        self::assertCount(1, $usingClause);

        $usingTable = $usingClause[0]->getRangeVar();
        self::assertNotNull($usingTable);
        self::assertSame('orders', $usingTable->getRelname());

        $usingAlias = $usingTable->getAlias();
        self::assertNotNull($usingAlias);
        self::assertSame('o', $usingAlias->getAliasname());
    }

    public function test_round_trip_with_where() : void
    {
        $original = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Literal::int(1)));

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();
        self::assertTrue($restoredAst->hasWhereClause());
        self::assertNotNull($restoredAst->getWhereClause());
    }

    public function test_simple_delete() : void
    {
        $query = DeleteBuilder::create()
            ->from('users');

        $ast = $query->toAst();
        self::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertNull($relation->getAlias());
    }

    public function test_simple_delete_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('users');

        $deparsed = $this->deparse($delete->toAst());
        self::assertSame('DELETE FROM users', $deparsed);
    }

    public function test_to_ast_without_table_throws_exception() : void
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('Cannot create DeleteStmt without table name. Call from() first.');

        $builder = DeleteBuilder::create();
        \assert($builder instanceof DeleteFinalStep);
        $builder->toAst();
    }

    private function deparse(DeleteStmt $deleteStmt) : string
    {
        $parser = new Parser();
        $node = new Node(['delete_stmt' => $deleteStmt]);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('DELETE FROM dummy');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
