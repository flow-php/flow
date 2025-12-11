<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\JoinType as ProtobufJoinType;
use Flow\PostgreSql\QueryBuilder\Condition\{Comparison, ComparisonOperator};
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Table\{AliasedTable, JoinType, JoinedTable, Table};
use PHPUnit\Framework\TestCase;

final class JoinedTableTest extends TestCase
{
    public function test_as_method_returns_aliased_table() : void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(
            Column::name('user_id'),
            ComparisonOperator::EQ,
            Column::name('id')
        );

        $joined = new JoinedTable($left, $right, JoinType::INNER, $condition);

        $aliased = $joined->as('j');

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame('j', $aliased->alias);
    }

    public function test_converts_inner_join_with_on_condition_to_ast() : void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(
            Column::name('user_id'),
            ComparisonOperator::EQ,
            Column::name('id')
        );

        $joined = new JoinedTable($left, $right, JoinType::INNER, $condition);

        $node = $joined->toAst();

        self::assertTrue($node->hasJoinExpr());

        $joinExpr = $node->getJoinExpr();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\JoinExpr::class, $joinExpr);
        self::assertSame(ProtobufJoinType::JOIN_INNER, $joinExpr->getJointype());

        $larg = $joinExpr->getLarg();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $larg);
        self::assertTrue($larg->hasRangeVar());
        $largRangeVar = $larg->getRangeVar();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $largRangeVar);
        self::assertSame('users', $largRangeVar->getRelname());

        $rarg = $joinExpr->getRarg();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $rarg);
        self::assertTrue($rarg->hasRangeVar());
        $rargRangeVar = $rarg->getRangeVar();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeVar::class, $rargRangeVar);
        self::assertSame('orders', $rargRangeVar->getRelname());

        $quals = $joinExpr->getQuals();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $quals);

        self::assertFalse($joinExpr->getIsNatural());
    }

    public function test_converts_left_join_to_ast() : void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(
            Column::name('user_id'),
            ComparisonOperator::EQ,
            Column::name('id')
        );

        $joined = new JoinedTable($left, $right, JoinType::LEFT, $condition);

        $node = $joined->toAst();

        $joinExpr = $node->getJoinExpr();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\JoinExpr::class, $joinExpr);
        self::assertSame(ProtobufJoinType::JOIN_LEFT, $joinExpr->getJointype());
    }

    public function test_converts_natural_join_to_ast() : void
    {
        $left = new Table('users');
        $right = new Table('orders');

        $joined = new JoinedTable($left, $right, JoinType::INNER, null, null, true);

        $node = $joined->toAst();

        $joinExpr = $node->getJoinExpr();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\JoinExpr::class, $joinExpr);
        self::assertTrue($joinExpr->getIsNatural());
        self::assertNull($joinExpr->getQuals());
    }

    public function test_converts_using_clause_to_ast() : void
    {
        $left = new Table('users');
        $right = new Table('orders');

        $joined = new JoinedTable($left, $right, JoinType::INNER, null, ['user_id']);

        $node = $joined->toAst();

        $joinExpr = $node->getJoinExpr();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\JoinExpr::class, $joinExpr);
        $usingClause = $joinExpr->getUsingClause();

        self::assertCount(1, $usingClause);

        $col = $usingClause[0]->getString();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBString::class, $col);
        self::assertSame('user_id', $col->getSval());
    }

    public function test_creates_joined_table_with_condition() : void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(
            Column::name('user_id'),
            ComparisonOperator::EQ,
            Column::name('id')
        );

        $joined = new JoinedTable($left, $right, JoinType::INNER, $condition);

        self::assertSame($left, $joined->left);
        self::assertSame($right, $joined->right);
        self::assertSame(JoinType::INNER, $joined->joinType);
        self::assertSame($condition, $joined->onCondition);
        self::assertNull($joined->usingColumns);
        self::assertFalse($joined->natural);
    }

    public function test_creates_joined_table_with_using_clause() : void
    {
        $left = new Table('users');
        $right = new Table('orders');

        $joined = new JoinedTable($left, $right, JoinType::INNER, null, ['user_id']);

        self::assertSame(['user_id'], $joined->usingColumns);
        self::assertNull($joined->onCondition);
    }

    public function test_reconstructs_joined_table_from_ast() : void
    {
        $left = new Table('users');
        $right = new Table('orders');
        $condition = new Comparison(
            Column::name('user_id'),
            ComparisonOperator::EQ,
            Column::name('id')
        );

        $original = new JoinedTable($left, $right, JoinType::LEFT, $condition);

        $node = $original->toAst();
        $reconstructed = JoinedTable::fromAst($node);

        self::assertInstanceOf(JoinedTable::class, $reconstructed);
        self::assertSame(JoinType::LEFT, $reconstructed->joinType);
        self::assertInstanceOf(Table::class, $reconstructed->left);
        self::assertInstanceOf(Table::class, $reconstructed->right);
        self::assertNotNull($reconstructed->onCondition);
    }

    public function test_reconstructs_joined_table_with_using_from_ast() : void
    {
        $left = new Table('users');
        $right = new Table('orders');

        $original = new JoinedTable($left, $right, JoinType::INNER, null, ['user_id', 'status']);

        $node = $original->toAst();
        $reconstructed = JoinedTable::fromAst($node);

        self::assertInstanceOf(JoinedTable::class, $reconstructed);
        self::assertSame(['user_id', 'status'], $reconstructed->usingColumns);
        self::assertNull($reconstructed->onCondition);
    }
}
