<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Delete;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\DeleteStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Clause\CTE;
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteBuilder;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteFinalStep;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use LogicException;
use PHPUnit\Framework\TestCase;

use function assert;
use function extension_loaded;
use function Flow\PostgreSql\DSL\any_;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;
use function function_exists;

final class DeleteBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_builder_steps_allow_fluent_interface(): void
    {
        $query = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'active'), ComparisonOperator::EQ, Literal::bool(false)))
            ->returning(Column::name('id'));

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);
    }

    public function test_delete_can_skip_optional_steps(): void
    {
        $queryWithoutWhere = DeleteBuilder::create()->from('temp_data')->returningAll();

        $ast = $queryWithoutWhere->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);
        static::assertFalse($ast->hasWhereClause());

        $returningList = $ast->getReturningList();
        static::assertCount(1, $returningList);
    }

    public function test_delete_complex_query(): void
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
                    Column::tableColumn('co', 'id'),
                ),
            )
            ->returning(
                Column::tableColumn('oi', 'id'),
                Column::tableColumn('oi', 'product_id'),
                Column::tableColumn('oi', 'quantity'),
            );

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);
        static::assertTrue($ast->hasWithClause());

        $usingClause = $ast->getUsingClause();
        static::assertNotNull($usingClause);
        static::assertCount(1, $usingClause);

        static::assertTrue($ast->hasWhereClause());

        $returningList = $ast->getReturningList();
        static::assertNotNull($returningList);
        static::assertCount(3, $returningList);
    }

    public function test_delete_from_schema_qualified_table_reference(): void
    {
        static::assertSame('DELETE FROM public.users', delete()->from(table('users', 'public'))->toSql());
    }

    public function test_delete_from_table_reference(): void
    {
        static::assertSame('DELETE FROM users', delete()->from(table('users'))->toSql());
    }

    public function test_delete_only_from_is_valid(): void
    {
        $query = DeleteBuilder::create()->from('all_data');

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('all_data', $relation->getRelname());
        static::assertFalse($ast->hasWhereClause());
        static::assertCount(0, $ast->getReturningList());
        static::assertCount(0, $ast->getUsingClause());
    }

    public function test_delete_returning_all(): void
    {
        $query = DeleteBuilder::create()->from('temp_data')->returningAll();

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $returningList = $ast->getReturningList();
        static::assertNotNull($returningList);
        static::assertCount(1, $returningList);

        $firstReturn = $returningList[0]->getResTarget();
        static::assertNotNull($firstReturn);

        $val = $firstReturn->getVal();
        static::assertNotNull($val);

        static::assertTrue($val->hasColumnRef());
        $columnRef = $val->getColumnRef();
        static::assertNotNull($columnRef);
        $fields = $columnRef->getFields();
        static::assertNotNull($fields);
        static::assertCount(1, $fields);
        static::assertTrue($fields[0]->hasAStar());
    }

    public function test_delete_with_alias(): void
    {
        $query = DeleteBuilder::create()->from('users', 'u');

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());

        $alias = $relation->getAlias();
        static::assertNotNull($alias);
        static::assertSame('u', $alias->getAliasname());
    }

    public function test_delete_with_alias_and_where(): void
    {
        $query = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Literal::int(1)));

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());

        $alias = $relation->getAlias();
        static::assertNotNull($alias);
        static::assertSame('u', $alias->getAliasname());
        static::assertTrue($ast->hasWhereClause());
    }

    public function test_delete_with_alias_and_where_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'active'), ComparisonOperator::EQ, Literal::bool(false)));

        $deparsed = $this->deparse($delete->toAst());
        static::assertSame('DELETE FROM users u WHERE u.active = false', $deparsed);
    }

    public function test_delete_with_multiple_using_tables(): void
    {
        $query = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using((new Table('orders'))->as('o'), (new Table('customers'))->as('c'))
            ->where((new Comparison(
                Column::tableColumn('oi', 'order_id'),
                ComparisonOperator::EQ,
                Column::tableColumn('o', 'id'),
            ))->and(
                new Comparison(
                    Column::tableColumn('o', 'customer_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('c', 'id'),
                ),
            )->and(
                new Comparison(Column::tableColumn('c', 'status'), ComparisonOperator::EQ, Literal::string('inactive')),
            ));

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $usingClause = $ast->getUsingClause();
        static::assertNotNull($usingClause);
        static::assertCount(2, $usingClause);

        $firstTable = $usingClause[0]->getRangeVar();
        static::assertNotNull($firstTable);
        static::assertSame('orders', $firstTable->getRelname());

        $firstAlias = $firstTable->getAlias();
        static::assertNotNull($firstAlias);
        static::assertSame('o', $firstAlias->getAliasname());

        $secondTable = $usingClause[1]->getRangeVar();
        static::assertNotNull($secondTable);
        static::assertSame('customers', $secondTable->getRelname());

        $secondAlias = $secondTable->getAlias();
        static::assertNotNull($secondAlias);
        static::assertSame('c', $secondAlias->getAliasname());
    }

    public function test_delete_with_multiple_using_tables_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using((new Table('orders'))->as('o'), (new Table('customers'))->as('c'))
            ->where((new Comparison(
                Column::tableColumn('oi', 'order_id'),
                ComparisonOperator::EQ,
                Column::tableColumn('o', 'id'),
            ))->and(
                new Comparison(
                    Column::tableColumn('o', 'customer_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('c', 'id'),
                ),
            ));

        $deparsed = $this->deparse($delete->toAst());
        static::assertSame(
            'DELETE FROM order_items oi USING orders o, customers c WHERE oi.order_id = o.id AND o.customer_id = c.id',
            $deparsed,
        );
    }

    public function test_delete_with_parameters(): void
    {
        $query = delete()->from('users')->where(eq(col('id'), param(1)));

        static::assertSame('DELETE FROM users WHERE id = $1', $query->toSql());
    }

    public function test_delete_with_returning(): void
    {
        $query = DeleteBuilder::create()
            ->from('sessions')
            ->where(new Comparison(Column::name('expires_at'), ComparisonOperator::LT, new FunctionCall(['now'])))
            ->returning(Column::name('id'), Column::name('user_id'));

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $returningList = $ast->getReturningList();
        static::assertNotNull($returningList);
        static::assertCount(2, $returningList);

        $firstReturn = $returningList[0]->getResTarget();
        static::assertNotNull($firstReturn);

        $firstVal = $firstReturn->getVal();
        static::assertNotNull($firstVal);
        static::assertTrue($firstVal->hasColumnRef());

        $secondReturn = $returningList[1]->getResTarget();
        static::assertNotNull($secondReturn);

        $secondVal = $secondReturn->getVal();
        static::assertNotNull($secondVal);
        static::assertTrue($secondVal->hasColumnRef());
    }

    public function test_delete_with_returning_all(): void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col('id'), literal(1)))
            ->returningAll();

        static::assertSame('DELETE FROM users WHERE id = 1 RETURNING *', $query->toSql());
    }

    public function test_delete_with_returning_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('sessions')
            ->where(new Comparison(Column::name('expired'), ComparisonOperator::EQ, Literal::bool(true)))
            ->returning(Column::name('id'));

        $deparsed = $this->deparse($delete->toAst());
        static::assertSame('DELETE FROM sessions WHERE expired = true RETURNING id', $deparsed);
    }

    public function test_delete_with_schema(): void
    {
        $query = DeleteBuilder::create()->from('myschema.users');

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertSame('myschema', $relation->getSchemaname());
    }

    public function test_delete_with_schema_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available.');
        }

        $query = DeleteBuilder::create()->from('public.users');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame('DELETE FROM public.users', $deparsed);
    }

    public function test_delete_with_schema_round_trip(): void
    {
        $original = DeleteBuilder::create()->from('myschema.users');

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        static::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        static::assertNotNull($restoredRelation);

        static::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());
        static::assertSame($originalRelation->getSchemaname(), $restoredRelation->getSchemaname());
    }

    public function test_delete_with_subquery_in_where(): void
    {
        $subquery = select()->select(col('user_id'))->from(table('inactive_users'));

        $query = delete()->from('users')->where(any_(col('id'), ComparisonOperator::EQ, $subquery));

        static::assertSame('DELETE FROM users WHERE id = ANY (SELECT user_id FROM inactive_users)', $query->toSql());
    }

    public function test_delete_with_using(): void
    {
        $query = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using(new Table('orders'))
            ->where(
                new Comparison(
                    Column::tableColumn('oi', 'order_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('orders', 'id'),
                ),
            );

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('order_items', $relation->getRelname());

        $alias = $relation->getAlias();
        static::assertNotNull($alias);
        static::assertSame('oi', $alias->getAliasname());

        $usingClause = $ast->getUsingClause();
        static::assertNotNull($usingClause);
        static::assertCount(1, $usingClause);

        $usingTable = $usingClause[0]->getRangeVar();
        static::assertNotNull($usingTable);
        static::assertSame('orders', $usingTable->getRelname());
    }

    public function test_delete_with_using_and_aliased_table(): void
    {
        $query = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using((new Table('orders'))->as('o'))
            ->where((new Comparison(
                Column::tableColumn('oi', 'order_id'),
                ComparisonOperator::EQ,
                Column::tableColumn('o', 'id'),
            ))->and(
                new Comparison(
                    Column::tableColumn('o', 'status'),
                    ComparisonOperator::EQ,
                    Literal::string('cancelled'),
                ),
            ));

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $usingClause = $ast->getUsingClause();
        static::assertNotNull($usingClause);
        static::assertCount(1, $usingClause);

        $usingTable = $usingClause[0]->getRangeVar();
        static::assertNotNull($usingTable);
        static::assertSame('orders', $usingTable->getRelname());
        $alias = $usingTable->getAlias();
        static::assertNotNull($alias);
        static::assertSame('o', $alias->getAliasname());
    }

    public function test_delete_with_using_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using((new Table('orders'))->as('o'))
            ->where((new Comparison(
                Column::tableColumn('oi', 'order_id'),
                ComparisonOperator::EQ,
                Column::tableColumn('o', 'id'),
            ))->and(
                new Comparison(
                    Column::tableColumn('o', 'status'),
                    ComparisonOperator::EQ,
                    Literal::string('cancelled'),
                ),
            ));

        $deparsed = $this->deparse($delete->toAst());
        static::assertSame(
            "DELETE FROM order_items oi USING orders o WHERE oi.order_id = o.id AND o.status = 'cancelled'",
            $deparsed,
        );
    }

    public function test_delete_with_where(): void
    {
        $query = DeleteBuilder::create()
            ->from('users')
            ->where(new Comparison(Column::name('active'), ComparisonOperator::EQ, Literal::bool(false)));

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);
        static::assertTrue($ast->hasWhereClause());
        static::assertNotNull($ast->getWhereClause());
    }

    public function test_delete_with_where_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()
            ->from('users')
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        $deparsed = $this->deparse($delete->toAst());
        static::assertSame('DELETE FROM users WHERE id = 1', $deparsed);
    }

    public function test_delete_with_with_clause(): void
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
                    Column::tableColumn('inactive_users', 'id'),
                ),
            );

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);
        static::assertTrue($ast->hasWithClause());

        $withClauseProto = $ast->getWithClause();
        static::assertNotNull($withClauseProto);

        $ctes = $withClauseProto->getCtes();
        static::assertNotNull($ctes);
        static::assertCount(1, $ctes);

        $firstCte = $ctes[0]->getCommonTableExpr();
        static::assertNotNull($firstCte);
        static::assertSame('inactive_users', $firstCte->getCtename());
    }

    public function test_delete_without_where_returning_all(): void
    {
        $query = DeleteBuilder::create()->from('temp_logs')->returningAll();

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('temp_logs', $relation->getRelname());
        static::assertFalse($ast->hasWhereClause());

        $returningList = $ast->getReturningList();
        static::assertNotNull($returningList);
        static::assertCount(1, $returningList);
    }

    public function test_from_ast_empty_relname_throws_exception(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "relname" in RangeVar node');

        $deleteStmt = new DeleteStmt();
        $deleteStmt->setRelation(new RangeVar([
            'relname' => '',
        ]));

        DeleteBuilder::fromAst($deleteStmt);
    }

    public function test_from_ast_missing_relation_throws_exception(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "relation" in DeleteStmt node');

        $deleteStmt = new DeleteStmt();
        DeleteBuilder::fromAst($deleteStmt);
    }

    public function test_immutability_from(): void
    {
        $original = DeleteBuilder::create();
        $modified = $original->from('users');

        static::assertNotSame($original, $modified);
    }

    public function test_immutability_returning(): void
    {
        $original = DeleteBuilder::create()->from('users');
        $modified = $original->returning(Column::name('id'));

        static::assertNotSame($original, $modified);
    }

    public function test_immutability_returning_all(): void
    {
        $original = DeleteBuilder::create()->from('users');
        $modified = $original->returningAll();

        static::assertNotSame($original, $modified);
    }

    public function test_immutability_using(): void
    {
        $original = DeleteBuilder::create()->from('users');
        $modified = $original->using(new Table('orders'));

        static::assertNotSame($original, $modified);
    }

    public function test_immutability_where(): void
    {
        $original = DeleteBuilder::create()->from('users');
        $modified = $original->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        static::assertNotSame($original, $modified);
    }

    public function test_round_trip_complex(): void
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
                    Column::tableColumn('co', 'id'),
                ),
            )
            ->returning(Column::name('id'), Column::name('product_id'));

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        static::assertTrue($restoredAst->hasWithClause());

        $withClauseProto = $restoredAst->getWithClause();
        static::assertNotNull($withClauseProto);

        $ctes = $withClauseProto->getCtes();
        static::assertNotNull($ctes);
        static::assertCount(1, $ctes);

        $usingClause = $restoredAst->getUsingClause();
        static::assertCount(1, $usingClause);

        $returningList = $restoredAst->getReturningList();
        static::assertCount(2, $returningList);
    }

    public function test_round_trip_simple(): void
    {
        $original = DeleteBuilder::create()->from('users');

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        static::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        static::assertNotNull($restoredRelation);

        static::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());
        static::assertSame($ast->hasWhereClause(), $restoredAst->hasWhereClause());
    }

    public function test_round_trip_with_alias(): void
    {
        $original = DeleteBuilder::create()->from('users', 'u');

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        static::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        static::assertNotNull($restoredRelation);

        static::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());

        $originalAlias = $originalRelation->getAlias();
        static::assertNotNull($originalAlias);

        $restoredAlias = $restoredRelation->getAlias();
        static::assertNotNull($restoredAlias);

        static::assertSame($originalAlias->getAliasname(), $restoredAlias->getAliasname());
    }

    public function test_round_trip_with_returning(): void
    {
        $original = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Literal::int(1)))
            ->returning(Column::name('id'));

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();
        static::assertTrue($restoredAst->hasWhereClause());

        $returningList = $restoredAst->getReturningList();
        static::assertNotNull($returningList);
        static::assertCount(1, $returningList);
    }

    public function test_round_trip_with_using(): void
    {
        $original = DeleteBuilder::create()
            ->from('order_items', 'oi')
            ->using((new Table('orders'))->as('o'))
            ->where(
                new Comparison(
                    Column::tableColumn('oi', 'order_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('o', 'id'),
                ),
            );

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();

        $usingClause = $restoredAst->getUsingClause();
        static::assertNotNull($usingClause);
        static::assertCount(1, $usingClause);

        $usingTable = $usingClause[0]->getRangeVar();
        static::assertNotNull($usingTable);
        static::assertSame('orders', $usingTable->getRelname());

        $usingAlias = $usingTable->getAlias();
        static::assertNotNull($usingAlias);
        static::assertSame('o', $usingAlias->getAliasname());
    }

    public function test_round_trip_with_where(): void
    {
        $original = DeleteBuilder::create()
            ->from('users', 'u')
            ->where(new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Literal::int(1)));

        $ast = $original->toAst();
        $restored = DeleteBuilder::fromAst($ast);

        $restoredAst = $restored->toAst();
        static::assertTrue($restoredAst->hasWhereClause());
        static::assertNotNull($restoredAst->getWhereClause());
    }

    public function test_simple_delete(): void
    {
        $query = DeleteBuilder::create()->from('users');

        $ast = $query->toAst();
        static::assertInstanceOf(DeleteStmt::class, $ast);

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertNull($relation->getAlias());
    }

    public function test_simple_delete_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $delete = DeleteBuilder::create()->from('users');

        $deparsed = $this->deparse($delete->toAst());
        static::assertSame('DELETE FROM users', $deparsed);
    }

    public function test_to_ast_without_table_throws_exception(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Cannot create DeleteStmt without table name. Call from() first.');

        $builder = DeleteBuilder::create();
        assert($builder instanceof DeleteFinalStep);
        $builder->toAst();
    }

    private function deparse(DeleteStmt $deleteStmt): string
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
