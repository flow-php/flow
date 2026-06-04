<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Update;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\Protobuf\AST\UpdateStmt;
use Flow\PostgreSql\QueryBuilder\Clause\CTE;
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\BinaryExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use Flow\PostgreSql\QueryBuilder\Update\UpdateBuilder;
use PHPUnit\Framework\TestCase;

use function count;
use function extension_loaded;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\sub_select;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\update;
use function function_exists;

final class UpdateBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_chaining_set_methods(): void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('first_name', Literal::string('John'))
            ->set('last_name', Literal::string('Doe'))
            ->set('email', Literal::string('john.doe@example.com'))
            ->setAll([]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        static::assertNotNull($targetList);
        static::assertCount(3, $targetList);
    }

    public function test_complex_update_with_all_clauses(): void
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

        static::assertNotNull($ast->getRelation());
        static::assertNotNull($ast->getTargetList());
        static::assertNotNull($ast->getFromClause());
        static::assertTrue($ast->hasWhereClause());
        static::assertNotNull($ast->getReturningClause()?->getExprs());
    }

    public function test_from_ast_throws_on_empty_target_list(): void
    {
        $this->expectException(InvalidAstException::class);

        $updateStmt = new UpdateStmt();
        $rangeVar = new RangeVar(['relname' => 'users']);
        $updateStmt->setRelation($rangeVar);

        UpdateBuilder::fromAst($updateStmt);
    }

    public function test_from_ast_throws_on_missing_relation(): void
    {
        $this->expectException(InvalidAstException::class);

        $updateStmt = new UpdateStmt();

        UpdateBuilder::fromAst($updateStmt);
    }

    public function test_immutability_from(): void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);
        $builder2 = $builder1->from(new Table('other_table'));

        static::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_returning(): void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);
        $builder2 = $builder1->returning(Column::name('id'));

        static::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_returning_all(): void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);
        $builder2 = $builder1->returningAll();

        static::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_set(): void
    {
        $builder1 = UpdateBuilder::create()->update('users');
        $builder2 = $builder1->set('name', Literal::string('John'));

        static::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_set_all(): void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'));
        $builder2 = $builder1->setAll(['email' => Literal::string('john@example.com')]);

        static::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_update(): void
    {
        $builder1 = UpdateBuilder::create();
        $builder2 = $builder1->update('users');

        static::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_where(): void
    {
        $builder1 = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);
        $builder2 = $builder1->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        static::assertNotSame($builder1, $builder2);
    }

    public function test_round_trip_complex(): void
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

        static::assertNotNull($restoredAst->getRelation());
        static::assertNotNull($restoredAst->getTargetList());
        static::assertNotNull($restoredAst->getFromClause());
        static::assertTrue($restoredAst->hasWhereClause());
        static::assertNotNull($restoredAst->getReturningClause()?->getExprs());
    }

    public function test_round_trip_simple(): void
    {
        $original = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);

        $ast = $original->toAst();

        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        static::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        static::assertNotNull($restoredRelation);

        static::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());

        $originalTargetList = $ast->getTargetList();
        $restoredTargetList = $restoredAst->getTargetList();
        static::assertCount(count($originalTargetList), $restoredTargetList);
    }

    public function test_round_trip_with_from(): void
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
        static::assertNotNull($fromClause);
        static::assertCount(1, $fromClause);
    }

    public function test_round_trip_with_returning(): void
    {
        $original = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll([])
            ->returning(Column::name('id'), Column::name('name'));

        $ast = $original->toAst();

        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $returningList = $restoredAst->getReturningClause()?->getExprs();
        static::assertNotNull($returningList);
        static::assertCount(2, $returningList);
    }

    public function test_round_trip_with_where(): void
    {
        $original = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll(['email' => Literal::string('john@example.com')])
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        $ast = $original->toAst();

        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        static::assertTrue($restoredAst->hasWhereClause());
    }

    public function test_simple_update(): void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll(['email' => Literal::string('john@example.com')]);

        $ast = $query->toAst();
        static::assertInstanceOf(UpdateStmt::class, $ast);

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertFalse($relation->hasAlias());

        $targetList = $ast->getTargetList();
        static::assertNotNull($targetList);
        static::assertCount(2, $targetList);

        $nameTarget = $targetList->offsetGet(0)->getResTarget();
        static::assertNotNull($nameTarget);
        static::assertSame('name', $nameTarget->getName());

        $emailTarget = $targetList->offsetGet(1)->getResTarget();
        static::assertNotNull($emailTarget);
        static::assertSame('email', $emailTarget->getName());
    }

    public function test_simple_update_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()->update('users')->set('name', Literal::string('John'))->setAll([]);

        $deparsed = $this->deparse($update->toAst());
        static::assertSame("UPDATE users SET name = 'John'", $deparsed);
    }

    public function test_throws_on_empty_assignments(): void
    {
        $this->expectException(InvalidExpressionException::class);
        $this->expectExceptionMessage('assignments cannot be empty');

        UpdateBuilder::create()->update('users')->toAst();
    }

    public function test_throws_on_missing_table(): void
    {
        $this->expectException(InvalidExpressionException::class);

        UpdateBuilder::create()->update('')->set('name', Literal::string('John'))->setAll([])->toAst();
    }

    public function test_update_combining_set_and_set_all(): void
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
        static::assertNotNull($targetList);
        static::assertCount(4, $targetList);
    }

    public function test_update_multiple_columns(): void
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
        static::assertNotNull($targetList);
        static::assertCount(3, $targetList);

        $priceTarget = $targetList->offsetGet(0)->getResTarget();
        static::assertNotNull($priceTarget);
        static::assertSame('price', $priceTarget->getName());

        $updatedAtTarget = $targetList->offsetGet(1)->getResTarget();
        static::assertNotNull($updatedAtTarget);
        static::assertSame('updated_at', $updatedAtTarget->getName());

        $versionTarget = $targetList->offsetGet(2)->getResTarget();
        static::assertNotNull($versionTarget);
        static::assertSame('version', $versionTarget->getName());
    }

    public function test_update_multiple_columns_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('products')
            ->set('price', Literal::float(99.99))
            ->set('stock', Literal::int(100))
            ->setAll([]);

        $deparsed = $this->deparse($update->toAst());
        static::assertSame('UPDATE products SET price = 99.99, stock = 100', $deparsed);
    }

    public function test_update_only_with_set_all(): void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->setAll([
                'first_name' => Literal::string('John'),
                'last_name' => Literal::string('Doe'),
            ]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        static::assertNotNull($targetList);
        static::assertCount(2, $targetList);
    }

    public function test_update_with_alias(): void
    {
        $query = UpdateBuilder::create()->update('users', 'u')->set('name', Literal::string('Jane'))->setAll([]);

        $ast = $query->toAst();
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertTrue($relation->hasAlias());

        $alias = $relation->getAlias();
        static::assertNotNull($alias);
        static::assertSame('u', $alias->getAliasname());
    }

    public function test_update_with_binary_expression(): void
    {
        $query = UpdateBuilder::create()
            ->update('counters')
            ->set('count', new BinaryExpression(Column::name('count'), '+', Literal::int(1)))
            ->setAll([]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        static::assertNotNull($targetList);

        $target = $targetList->offsetGet(0)->getResTarget();
        static::assertNotNull($target);
        static::assertSame('count', $target->getName());

        $val = $target->getVal();
        static::assertNotNull($val);
        static::assertTrue($val->hasAExpr());
    }

    public function test_update_with_binary_expression_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('counters')
            ->set('count', new BinaryExpression(Column::name('count'), '+', Literal::int(1)))
            ->setAll([]);

        $deparsed = $this->deparse($update->toAst());
        static::assertSame('UPDATE counters SET count = count + 1', $deparsed);
    }

    public function test_update_with_column_expression(): void
    {
        $query = update()
            ->update('products')
            ->set('price', col('price'))
            ->where(eq(col('id'), literal(1)));

        static::assertSame('UPDATE products SET price = price WHERE id = 1', $query->toSql());
    }

    public function test_update_with_column_reference(): void
    {
        $query = UpdateBuilder::create()
            ->update('employees')
            ->set('manager_id', Column::name('senior_manager_id'))
            ->setAll([]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        static::assertNotNull($targetList);

        $target = $targetList->offsetGet(0)->getResTarget();
        static::assertNotNull($target);
        static::assertSame('manager_id', $target->getName());

        $val = $target->getVal();
        static::assertNotNull($val);
        static::assertTrue($val->hasColumnRef());
    }

    public function test_update_with_from(): void
    {
        $query = UpdateBuilder::create()
            ->update('orders', 'o')
            ->set('status', Column::name('s.status'))
            ->setAll([])
            ->from((new Table('order_status'))->as('s'))
            ->where(new Comparison(Column::name('o.status_id'), ComparisonOperator::EQ, Column::name('s.id')));

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();
        static::assertNotNull($fromClause);
        static::assertCount(1, $fromClause);

        $fromNode = $fromClause->offsetGet(0);
        static::assertTrue($fromNode->hasRangeVar());

        $rangeVar = $fromNode->getRangeVar();
        static::assertNotNull($rangeVar);
        static::assertSame('order_status', $rangeVar->getRelname());

        $alias = $rangeVar->getAlias();
        static::assertNotNull($alias);
        static::assertSame('s', $alias->getAliasname());
    }

    public function test_update_with_from_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('orders', 'o')
            ->set('status', Column::tableColumn('s', 'status'))
            ->setAll([])
            ->from((new Table('order_status'))->as('s'))
            ->where(
                new Comparison(
                    Column::tableColumn('o', 'status_id'),
                    ComparisonOperator::EQ,
                    Column::tableColumn('s', 'id'),
                ),
            );

        $deparsed = $this->deparse($update->toAst());
        static::assertSame(
            'UPDATE orders o SET status = s.status FROM order_status s WHERE o.status_id = s.id',
            $deparsed,
        );
    }

    public function test_update_with_function_call(): void
    {
        $query = UpdateBuilder::create()
            ->update('logs')
            ->set('logged_at', new FunctionCall(['now']))
            ->setAll([]);

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();
        static::assertNotNull($targetList);

        $target = $targetList->offsetGet(0)->getResTarget();
        static::assertNotNull($target);
        static::assertSame('logged_at', $target->getName());

        $val = $target->getVal();
        static::assertNotNull($val);
        static::assertTrue($val->hasFuncCall());
    }

    public function test_update_with_multiple_from_tables(): void
    {
        $query = UpdateBuilder::create()
            ->update('products', 'p')
            ->set('price', Column::name('pp.price'))
            ->setAll([])
            ->from((new Table('product_prices'))->as('pp'), (new Table('categories'))->as('c'));

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();
        static::assertNotNull($fromClause);
        static::assertCount(2, $fromClause);
    }

    public function test_update_with_multiple_set(): void
    {
        $query = update()
            ->update('users')
            ->set('name', literal('John'))
            ->set('email', literal('john@example.com'))
            ->where(eq(col('id'), literal(1)));

        static::assertSame("UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1", $query->toSql());
    }

    public function test_update_with_parameters(): void
    {
        $query = update()
            ->update('users')
            ->set('name', param(1))
            ->where(eq(col('id'), param(2)));

        static::assertSame('UPDATE users SET name = $1 WHERE id = $2', $query->toSql());
    }

    public function test_update_with_returning(): void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('updated_at', new FunctionCall(['now']))
            ->setAll([])
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)))
            ->returning(Column::name('id'), Column::name('updated_at'));

        $ast = $query->toAst();
        $returningList = $ast->getReturningClause()?->getExprs();
        static::assertNotNull($returningList);
        static::assertCount(2, $returningList);
    }

    public function test_update_with_returning_all(): void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('name', Literal::string('John'))
            ->setAll([])
            ->returningAll();

        $ast = $query->toAst();
        $returningList = $ast->getReturningClause()?->getExprs();
        static::assertNotNull($returningList);
        static::assertCount(1, $returningList);

        $returningNode = $returningList->offsetGet(0);
        $resTarget = $returningNode->getResTarget();
        static::assertNotNull($resTarget);
        $val = $resTarget->getVal();
        static::assertNotNull($val);

        static::assertTrue($val->hasColumnRef());
        $columnRef = $val->getColumnRef();
        static::assertNotNull($columnRef);
        $fields = $columnRef->getFields();
        static::assertNotNull($fields);
        static::assertCount(1, $fields);
        static::assertTrue($fields[0]->hasAStar());
    }

    public function test_update_with_returning_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('users')
            ->set('last_login', new FunctionCall(['now']))
            ->setAll([])
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)))
            ->returning(Column::name('id'), Column::name('last_login'));

        $deparsed = $this->deparse($update->toAst());
        static::assertSame('UPDATE users SET last_login = now() WHERE id = 1 RETURNING id, last_login', $deparsed);
    }

    public function test_update_with_schema(): void
    {
        $query = UpdateBuilder::create()->update('myschema.users')->set('name', Literal::string('John'))->setAll([]);

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertSame('myschema', $relation->getSchemaname());
    }

    public function test_update_with_schema_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available.');
        }

        $query = UpdateBuilder::create()->update('public.users')->set('name', Literal::string('John'))->setAll([]);

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("UPDATE public.users SET name = 'John'", $deparsed);
    }

    public function test_update_with_schema_qualified_table_reference(): void
    {
        static::assertSame(
            "UPDATE public.users SET name = 'John'",
            update()->update(table('users', 'public'))->set('name', literal('John'))->toSql(),
        );
    }

    public function test_update_with_schema_round_trip(): void
    {
        $original = UpdateBuilder::create()->update('myschema.users')->set('name', Literal::string('John'))->setAll([]);

        $ast = $original->toAst();
        $restored = UpdateBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        $originalRelation = $ast->getRelation();
        static::assertNotNull($originalRelation);

        $restoredRelation = $restoredAst->getRelation();
        static::assertNotNull($restoredRelation);

        static::assertSame($originalRelation->getRelname(), $restoredRelation->getRelname());
        static::assertSame($originalRelation->getSchemaname(), $restoredRelation->getSchemaname());
    }

    public function test_update_with_set_all(): void
    {
        $query = update()
            ->update('users')
            ->setAll([
                'name' => literal('John'),
                'email' => literal('john@example.com'),
            ])
            ->where(eq(col('id'), literal(1)));

        static::assertSame("UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1", $query->toSql());
    }

    public function test_update_with_subquery_in_set(): void
    {
        $subquery = select()
            ->select(col('avg_price'))
            ->from(table('price_stats'))
            ->where(eq(col('category'), col('products.category')));

        $query = update()
            ->update('products')
            ->set('price', sub_select($subquery))
            ->where(eq(col('id'), literal(1)));

        static::assertSame(
            'UPDATE products SET price = (SELECT avg_price FROM price_stats WHERE category = products.category) WHERE id = 1',
            $query->toSql(),
        );
    }

    public function test_update_with_table_reference(): void
    {
        static::assertSame(
            "UPDATE users SET name = 'John'",
            update()->update(table('users'))->set('name', literal('John'))->toSql(),
        );
    }

    public function test_update_with_where(): void
    {
        $query = UpdateBuilder::create()
            ->update('users')
            ->set('active', Literal::bool(false))
            ->setAll([])
            ->where(new Comparison(Column::name('last_login'), ComparisonOperator::LT, Literal::string('2024-01-01')));

        $ast = $query->toAst();
        static::assertTrue($ast->hasWhereClause());

        $whereClause = $ast->getWhereClause();
        static::assertNotNull($whereClause);
        static::assertTrue($whereClause->hasAExpr());
    }

    public function test_update_with_where_deparsed_output(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $update = UpdateBuilder::create()
            ->update('users')
            ->set('status', Literal::string('active'))
            ->setAll([])
            ->where(new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(1)));

        $deparsed = $this->deparse($update->toAst());
        static::assertSame("UPDATE users SET status = 'active' WHERE id = 1", $deparsed);
    }

    public function test_update_with_with_clause(): void
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
        static::assertTrue($ast->hasWithClause());

        $withClause = $ast->getWithClause();
        static::assertNotNull($withClause);

        $ctes = $withClause->getCtes();
        static::assertNotNull($ctes);
        static::assertCount(1, $ctes);
    }

    private function deparse(UpdateStmt $updateStmt): string
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
