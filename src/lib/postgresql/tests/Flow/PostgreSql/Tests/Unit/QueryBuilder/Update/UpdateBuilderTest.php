<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Update;

use Flow\PostgreSql\{ParsedQuery, Parser};
use Flow\PostgreSql\Protobuf\AST\{Node, RawStmt, UpdateStmt};
use Flow\PostgreSql\QueryBuilder\Clause\{CTE, WithClause};
use Flow\PostgreSql\QueryBuilder\Condition\{Comparison, ComparisonOperator};
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, InvalidExpressionException};
use Flow\PostgreSql\QueryBuilder\Expression\{BinaryExpression, Column, FunctionCall, Literal};
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use Flow\PostgreSql\QueryBuilder\Update\UpdateBuilder;
use PHPUnit\Framework\TestCase;

final class UpdateBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_chaining_set_methods() : void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('first_name', Literal::string('John'))
            ->set('last_name', Literal::string('Doe'))
            ->set('email', Literal::string('john.doe@example.com'))
            ->setAll([]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);
        self::assertCount(3, $targetList);
    }

    public function test_complex_update_with_all_clauses() : void
    {
        $query = UpdateBuilder::create()
            ->update('orders', 'o')
            ->set('status', Literal::string('shipped'))
            ->set('shipped_at', new FunctionCall(['now']))
            ->setAll([])
            ->from((new Table('shipments'))->as('s'))
            ->where(new Comparison(Column::name('o.id'), ComparisonOperator::EQ, Column::name('s.order_id')))
            ->returning(Column::name('o.id'), Column::name('o.status'));

        $ast = $query->toAst();

        self::assertNotNull($ast->getRelation());
        self::assertNotNull($ast->getTargetList());
        self::assertNotNull($ast->getFromClause());
        self::assertTrue($ast->hasWhereClause());
        self::assertNotNull($ast->getReturningList());
    }

    public function test_from_ast_throws_on_empty_target_list() : void
    {
        $this->expectException(InvalidAstException::class);

        $updateStmt = new UpdateStmt();
        $rangeVar = new \Flow\PostgreSql\Protobuf\AST\RangeVar(['relname' => 'users']);
        $updateStmt->setRelation($rangeVar);

        UpdateBuilder::fromAst($updateStmt);
    }

    public function test_from_ast_throws_on_missing_relation() : void
    {
        $this->expectException(InvalidAstException::class);

        $updateStmt = new UpdateStmt();

        UpdateBuilder::fromAst($updateStmt);
    }

    public function test_immutability_from() : void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);
        $builder2 = $builder1->from(new Table('other_table'));

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_returning() : void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);
        $builder2 = $builder1->returning(Column::name('id'));

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_returning_all() : void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);
        $builder2 = $builder1->returningAll();

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_set() : void
    {
        $builder1 = UpdateBuilder::create()->update('users');
        $builder2 = $builder1->set('name', Literal::string('John'));

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_set_all() : void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'));
        $builder2 = $builder1->setAll(['email' => Literal::string('john@example.com')]);

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_update() : void
    {
        $builder1 = UpdateBuilder::create();
        $builder2 = $builder1->update('users');

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_where() : void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);
        $builder2 = $builder1->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        self::assertNotSame($builder1, $builder2);
    }

    public function test_round_trip_complex() : void
    {
        $original = UpdateBuilder::create()
            ->update('orders', 'o')
            ->set('status', Literal::string('completed'))
            ->set('completed_at', new FunctionCall(['now']))
            ->setAll([])
            ->from((new Table('order_items'))->as('oi'))
            ->where(new Comparison(Column::name('o.id'), ComparisonOperator::EQ, Column::name('oi.order_id')))
            ->returning(Column::name('o.id'));

        $ast = $original->toAst();

        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        self::assertNotNull($restoredAst->getRelation());
        self::assertNotNull($restoredAst->getTargetList());
        self::assertNotNull($restoredAst->getFromClause());
        self::assertTrue($restoredAst->hasWhereClause());
        self::assertNotNull($restoredAst->getReturningList());
    }

    public function test_round_trip_simple() : void
    {
        $original = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll([]);

        $ast = $original->toAst();

        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        self::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        self::assertNotNull($restoredRelation);

        self::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());

        $originalTargetList = $ast->getTargetList();
        $restoredTargetList = $restoredAst->getTargetList();
        self::assertCount(\count($originalTargetList), $restoredTargetList);
    }

    public function test_round_trip_with_from() : void
    {
        $original = UpdateBuilder::create()
            ->update('orders', 'o')
            ->set('status', Column::name('s.status'))
            ->setAll([])
            ->from((new Table('order_status'))->as('s'));

        $ast = $original->toAst();

        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $fromClause = $restoredAst->getFromClause();
        self::assertNotNull($fromClause);
        self::assertCount(1, $fromClause);
    }

    public function test_round_trip_with_returning() : void
    {
        $original = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll([])
            ->returning(Column::name('id'), Column::name('name'));

        $ast = $original->toAst();

        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $returningList = $restoredAst->getReturningList();
        self::assertNotNull($returningList);
        self::assertCount(2, $returningList);
    }

    public function test_round_trip_with_where() : void
    {
        $original = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll(['email' => Literal::string('john@example.com')])
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        $ast = $original->toAst();

        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        self::assertTrue($restoredAst->hasWhereClause());
    }

    public function test_simple_update() : void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll(['email' => Literal::string('john@example.com')]);

        $ast = $query->toAst();
        self::assertInstanceOf(UpdateStmt::class, $ast);

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertFalse($relation->hasAlias());

        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);
        self::assertCount(2, $targetList);

        $nameTarget = $targetList->offsetGet(0)->getResTarget();
        self::assertNotNull($nameTarget);
        self::assertSame('name', $nameTarget->getName());

        $emailTarget = $targetList->offsetGet(1)->getResTarget();
        self::assertNotNull($emailTarget);
        self::assertSame('email', $emailTarget->getName());
    }

    public function test_simple_update_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll([]);

        $deparsed = $this->deparse($update->toAst());
        self::assertSame("UPDATE users SET name = 'John'", $deparsed);
    }

    public function test_throws_on_empty_assignments() : void
    {
        $this->expectException(InvalidExpressionException::class);
        $this->expectExceptionMessage('assignments cannot be empty');

        UpdateBuilder::create()
            ->update('users')
            ->toAst();
    }

    public function test_throws_on_missing_table() : void
    {
        $this->expectException(InvalidExpressionException::class);

        UpdateBuilder::create()
            ->update('')
            ->set('name', Literal::string('John'))
            ->setAll([])
            ->toAst();
    }

    public function test_update_combining_set_and_set_all() : void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('first_name', Literal::string('John'))
            ->set('last_name', Literal::string('Doe'))
            ->setAll([
                'email' => Literal::string('john.doe@example.com'),
                'updated_at' => new FunctionCall(['now']),
            ]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);
        self::assertCount(4, $targetList);
    }

    public function test_update_multiple_columns() : void
    {
        $query = UpdateBuilder::create()
            ->update('products')
            ->setAll([
                'price' => Literal::float(99.99),
                'updated_at' => new FunctionCall(['now']),
                'version' => new BinaryExpression(Column::name('version'), '+', Literal::int(1)),
            ]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);
        self::assertCount(3, $targetList);

        $priceTarget = $targetList->offsetGet(0)->getResTarget();
        self::assertNotNull($priceTarget);
        self::assertSame('price', $priceTarget->getName());

        $updatedAtTarget = $targetList->offsetGet(1)->getResTarget();
        self::assertNotNull($updatedAtTarget);
        self::assertSame('updated_at', $updatedAtTarget->getName());

        $versionTarget = $targetList->offsetGet(2)->getResTarget();
        self::assertNotNull($versionTarget);
        self::assertSame('version', $versionTarget->getName());
    }

    public function test_update_multiple_columns_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('products')
            ->set('price', Literal::float(99.99))
            ->set('stock', Literal::int(100))
            ->setAll([]);

        $deparsed = $this->deparse($update->toAst());
        self::assertSame('UPDATE products SET price = 99.99, stock = 100', $deparsed);
    }

    public function test_update_only_with_set_all() : void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->setAll([
                'first_name' => Literal::string('John'),
                'last_name' => Literal::string('Doe'),
            ]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);
        self::assertCount(2, $targetList);
    }

    public function test_update_with_alias() : void
    {
        $query = UpdateBuilder::create()
            ->update('users', 'u')
            ->set('name', Literal::string('Jane'))
            ->setAll([]);

        $ast = $query->toAst();
        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertTrue($relation->hasAlias());

        $alias = $relation->getAlias();
        self::assertNotNull($alias);
        self::assertSame('u', $alias->getAliasname());
    }

    public function test_update_with_binary_expression() : void
    {
        $query = UpdateBuilder::create()
            ->update('counters')
            ->set('count', new BinaryExpression(Column::name('count'), '+', Literal::int(1)))
            ->setAll([]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);

        $target = $targetList->offsetGet(0)->getResTarget();
        self::assertNotNull($target);
        self::assertSame('count', $target->getName());

        $val = $target->getVal();
        self::assertNotNull($val);
        self::assertTrue($val->hasAExpr());
    }

    public function test_update_with_binary_expression_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('counters')
            ->set('count', new BinaryExpression(Column::name('count'), '+', Literal::int(1)))
            ->setAll([]);

        $deparsed = $this->deparse($update->toAst());
        self::assertSame('UPDATE counters SET count = count + 1', $deparsed);
    }

    public function test_update_with_column_reference() : void
    {
        $query = UpdateBuilder::create()
            ->update('employees')
            ->set('manager_id', Column::name('senior_manager_id'))
            ->setAll([]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);

        $target = $targetList->offsetGet(0)->getResTarget();
        self::assertNotNull($target);
        self::assertSame('manager_id', $target->getName());

        $val = $target->getVal();
        self::assertNotNull($val);
        self::assertTrue($val->hasColumnRef());
    }

    public function test_update_with_from() : void
    {
        $query = UpdateBuilder::create()
            ->update('orders', 'o')
            ->set('status', Column::name('s.status'))
            ->setAll([])
            ->from((new Table('order_status'))->as('s'))
            ->where(new Comparison(Column::name('o.status_id'), ComparisonOperator::EQ, Column::name('s.id')));

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();
        self::assertNotNull($fromClause);
        self::assertCount(1, $fromClause);

        $fromNode = $fromClause->offsetGet(0);
        self::assertTrue($fromNode->hasRangeVar());

        $rangeVar = $fromNode->getRangeVar();
        self::assertNotNull($rangeVar);
        self::assertSame('order_status', $rangeVar->getRelname());

        $alias = $rangeVar->getAlias();
        self::assertNotNull($alias);
        self::assertSame('s', $alias->getAliasname());
    }

    public function test_update_with_from_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('orders', 'o')
            ->set('status', Column::tableColumn('s', 'status'))
            ->setAll([])
            ->from((new Table('order_status'))->as('s'))
            ->where(new Comparison(Column::tableColumn('o', 'status_id'), ComparisonOperator::EQ, Column::tableColumn('s', 'id')));

        $deparsed = $this->deparse($update->toAst());
        self::assertSame('UPDATE orders o SET status = s.status FROM order_status s WHERE o.status_id = s.id', $deparsed);
    }

    public function test_update_with_function_call() : void
    {
        $query = UpdateBuilder::create()
            ->update('logs')
            ->set('logged_at', new FunctionCall(['now']))
            ->setAll([]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);

        $target = $targetList->offsetGet(0)->getResTarget();
        self::assertNotNull($target);
        self::assertSame('logged_at', $target->getName());

        $val = $target->getVal();
        self::assertNotNull($val);
        self::assertTrue($val->hasFuncCall());
    }

    public function test_update_with_multiple_from_tables() : void
    {
        $query = UpdateBuilder::create()
            ->update('products', 'p')
            ->set('price', Column::name('pp.price'))
            ->setAll([])
            ->from(
                (new Table('product_prices'))->as('pp'),
                (new Table('categories'))->as('c')
            );

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();
        self::assertNotNull($fromClause);
        self::assertCount(2, $fromClause);
    }

    public function test_update_with_returning() : void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('updated_at', new FunctionCall(['now']))
            ->setAll([])
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)))
            ->returning(Column::name('id'), Column::name('updated_at'));

        $ast = $query->toAst();
        $returningList = $ast->getReturningList();
        self::assertNotNull($returningList);
        self::assertCount(2, $returningList);
    }

    public function test_update_with_returning_all() : void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll([])
            ->returningAll();

        $ast = $query->toAst();
        $returningList = $ast->getReturningList();
        self::assertNotNull($returningList);
        self::assertCount(1, $returningList);

        $returningNode = $returningList->offsetGet(0);
        $resTarget = $returningNode->getResTarget();
        self::assertNotNull($resTarget);
        $val = $resTarget->getVal();
        self::assertNotNull($val);

        self::assertTrue($val->hasColumnRef());
        $columnRef = $val->getColumnRef();
        self::assertNotNull($columnRef);
        $fields = $columnRef->getFields();
        self::assertNotNull($fields);
        self::assertCount(1, $fields);
        self::assertTrue($fields[0]->hasAStar());
    }

    public function test_update_with_returning_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('users')
            ->set('last_login', new FunctionCall(['now']))
            ->setAll([])
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)))
            ->returning(Column::name('id'), Column::name('last_login'));

        $deparsed = $this->deparse($update->toAst());
        self::assertSame('UPDATE users SET last_login = now() WHERE id = 1 RETURNING id, last_login', $deparsed);
    }

    public function test_update_with_schema() : void
    {
        $query = UpdateBuilder::create()
            ->update('myschema.users')
            ->set('name', Literal::string('John'))
            ->setAll([]);

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertSame('myschema', $relation->getSchemaname());
    }

    public function test_update_with_schema_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available.');
        }

        $query = UpdateBuilder::create()
            ->update('public.users')
            ->set('name', Literal::string('John'))
            ->setAll([]);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("UPDATE public.users SET name = 'John'", $deparsed);
    }

    public function test_update_with_schema_round_trip() : void
    {
        $original = UpdateBuilder::create()
            ->update('myschema.users')
            ->set('name', Literal::string('John'))
            ->setAll([]);

        $ast = $original->toAst();
        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        self::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        self::assertNotNull($restoredRelation);

        self::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());
        self::assertSame($originalRelation->getSchemaname(), $restoredRelation->getSchemaname());
    }

    public function test_update_with_where() : void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('active', Literal::bool(false))
            ->setAll([])
            ->where(new Comparison(Column::name('last_login'), ComparisonOperator::LT, Literal::string('2024-01-01')));

        $ast = $query->toAst();
        self::assertTrue($ast->hasWhereClause());

        $whereClause = $ast->getWhereClause();
        self::assertNotNull($whereClause);
        self::assertTrue($whereClause->hasAExpr());
    }

    public function test_update_with_where_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('users')
            ->set('status', Literal::string('active'))
            ->setAll([])
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        $deparsed = $this->deparse($update->toAst());
        self::assertSame("UPDATE users SET status = 'active' WHERE id = 1", $deparsed);
    }

    public function test_update_with_with_clause() : void
    {
        $selectStmt = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('users'))
            ->where(new Comparison(Column::name('active'), ComparisonOperator::EQ, Literal::bool(true)))
            ->toAst();

        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $cte = new CTE('updated_users', $selectNode);

        $query = UpdateBuilder::with(new WithClause([$cte]))
            ->update('orders')
            ->set('processed', Literal::bool(true))
            ->setAll([]);

        $ast = $query->toAst();
        self::assertTrue($ast->hasWithClause());

        $withClause = $ast->getWithClause();
        self::assertNotNull($withClause);

        $ctes = $withClause->getCtes();
        self::assertNotNull($ctes);
        self::assertCount(1, $ctes);
    }

    private function deparse(UpdateStmt $updateStmt) : string
    {
        $parser = new Parser();
        $node = new Node();
        $node->setUpdateStmt($updateStmt);
        $rawStmt = new RawStmt();
        $rawStmt->setStmt($node);
        $parsed = $parser->parse('UPDATE dummy SET x = 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
