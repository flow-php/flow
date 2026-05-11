<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\JoinType as ProtobufJoinType;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Table\AliasedTable;
use Flow\PostgreSql\QueryBuilder\Table\JoinedTable;
use Flow\PostgreSql\QueryBuilder\Table\JoinType;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class JoinedTableTest extends TestCase
{
    public function test_as_method_returns_aliased_table(): void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(Column::name('user_id'), ComparisonOperator::EQ, Column::name('id'));

        $joined = new JoinedTable($left, $right, JoinType::INNER, $condition);

        $aliased = $joined->as('j');

        static::assertInstanceOf(AliasedTable::class, $aliased);
        static::assertSame('j', $aliased->alias);
    }

    public function test_converts_inner_join_with_on_condition_to_ast(): void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(Column::name('user_id'), ComparisonOperator::EQ, Column::name('id'));

        $joined = new JoinedTable($left, $right, JoinType::INNER, $condition);

        $node = $joined->toAst();

        static::assertTrue($node->hasJoinExpr());

        $joinExpr = $node->getJoinExpr();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\JoinExpr::class, $joinExpr);
        static::assertSame(ProtobufJoinType::JOIN_INNER, $joinExpr->getJointype());

        $larg = $joinExpr->getLarg();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $larg);
        static::assertTrue($larg->hasRangeVar());
        $largRangeVar = $larg->getRangeVar();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $largRangeVar);
        static::assertSame('users', $largRangeVar->getRelname());

        $rarg = $joinExpr->getRarg();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $rarg);
        static::assertTrue($rarg->hasRangeVar());
        $rargRangeVar = $rarg->getRangeVar();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $rargRangeVar);
        static::assertSame('orders', $rargRangeVar->getRelname());

        $quals = $joinExpr->getQuals();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $quals);

        static::assertFalse($joinExpr->getIsNatural());
    }

    public function test_converts_left_join_to_ast(): void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(Column::name('user_id'), ComparisonOperator::EQ, Column::name('id'));

        $joined = new JoinedTable($left, $right, JoinType::LEFT, $condition);

        $node = $joined->toAst();

        $joinExpr = $node->getJoinExpr();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\JoinExpr::class, $joinExpr);
        static::assertSame(ProtobufJoinType::JOIN_LEFT, $joinExpr->getJointype());
    }

    public function test_converts_natural_join_to_ast(): void
    {
        $left = new Table('users');
        $right = new Table('orders');

        $joined = new JoinedTable($left, $right, JoinType::INNER, null, null, true);

        $node = $joined->toAst();

        $joinExpr = $node->getJoinExpr();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\JoinExpr::class, $joinExpr);
        static::assertTrue($joinExpr->getIsNatural());
        static::assertNull($joinExpr->getQuals());
    }

    public function test_converts_using_clause_to_ast(): void
    {
        $left = new Table('users');
        $right = new Table('orders');

        $joined = new JoinedTable($left, $right, JoinType::INNER, null, ['user_id']);

        $node = $joined->toAst();

        $joinExpr = $node->getJoinExpr();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\JoinExpr::class, $joinExpr);
        $usingClause = $joinExpr->getUsingClause();

        static::assertCount(1, $usingClause);

        $col = $usingClause[0]->getString();
        static::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBString::class, $col);
        static::assertSame('user_id', $col->getSval());
    }

    public function test_creates_joined_table_with_condition(): void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(Column::name('user_id'), ComparisonOperator::EQ, Column::name('id'));

        $joined = new JoinedTable($left, $right, JoinType::INNER, $condition);

        static::assertSame($left, $joined->left);
        static::assertSame($right, $joined->right);
        static::assertSame(JoinType::INNER, $joined->joinType);
        static::assertSame($condition, $joined->onCondition);
        static::assertNull($joined->usingColumns);
        static::assertFalse($joined->natural);
    }

    public function test_creates_joined_table_with_using_clause(): void
    {
        $left = new Table('users');
        $right = new Table('orders');

        $joined = new JoinedTable($left, $right, JoinType::INNER, null, ['user_id']);

        static::assertSame(['user_id'], $joined->usingColumns);
        static::assertNull($joined->onCondition);
    }

    public function test_reconstructs_joined_table_from_ast(): void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(Column::name('user_id'), ComparisonOperator::EQ, Column::name('id'));

        $original = new JoinedTable($left, $right, JoinType::LEFT, $condition);

        $node = $original->toAst();
        $reconstructed = JoinedTable::fromAst($node);

        static::assertInstanceOf(JoinedTable::class, $reconstructed);
        static::assertSame(JoinType::LEFT, $reconstructed->joinType);
        static::assertInstanceOf(Table::class, $reconstructed->left);
        static::assertInstanceOf(Table::class, $reconstructed->right);
        static::assertNotNull($reconstructed->onCondition);
    }

    public function test_reconstructs_joined_table_with_using_from_ast(): void
    {
        $left = new Table('users');
        $right = new Table('orders');

        $original = new JoinedTable($left, $right, JoinType::INNER, null, ['user_id', 'status']);

        $node = $original->toAst();
        $reconstructed = JoinedTable::fromAst($node);

        static::assertInstanceOf(JoinedTable::class, $reconstructed);
        static::assertSame(['user_id', 'status'], $reconstructed->usingColumns);
        static::assertNull($reconstructed->onCondition);
    }
}
