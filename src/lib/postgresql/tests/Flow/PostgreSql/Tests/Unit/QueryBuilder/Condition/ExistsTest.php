<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\SubLinkType;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\Exists;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use PHPUnit\Framework\TestCase;

final class ExistsTest extends TestCase
{
    public function test_and_method_returns_and_condition(): void
    {
        $subquery1 = new Node(['select_stmt' => new SelectStmt()]);
        $subquery2 = new Node(['select_stmt' => new SelectStmt()]);

        $condition1 = new Exists($subquery1);
        $condition2 = new Exists($subquery2);

        $result = $condition1->and($condition2);

        static::assertInstanceOf(AndCondition::class, $result);
    }

    public function test_converts_to_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $condition = new Exists($subquery);

        $node = $condition->toAst();

        static::assertTrue($node->hasSubLink());

        $subLink = $node->getSubLink();
        static::assertNotNull($subLink);
        static::assertSame(SubLinkType::EXISTS_SUBLINK, $subLink->getSubLinkType());
        static::assertTrue($subLink->hasSubselect());
    }

    public function test_creates_exists_condition(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);

        $condition = new Exists($subquery);

        static::assertInstanceOf(Exists::class, $condition);
        static::assertSame($subquery, $condition->subquery);
    }

    public function test_not_method_returns_not_condition(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $condition = new Exists($subquery);

        $result = $condition->not();

        static::assertInstanceOf(NotCondition::class, $result);
    }

    public function test_or_method_returns_or_condition(): void
    {
        $subquery1 = new Node(['select_stmt' => new SelectStmt()]);
        $subquery2 = new Node(['select_stmt' => new SelectStmt()]);

        $condition1 = new Exists($subquery1);
        $condition2 = new Exists($subquery2);

        $result = $condition1->or($condition2);

        static::assertInstanceOf(OrCondition::class, $result);
    }

    public function test_reconstructs_from_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $original = new Exists($subquery);

        $node = $original->toAst();
        $reconstructed = Exists::fromAst($node);

        static::assertInstanceOf(Exists::class, $reconstructed);
        static::assertInstanceOf(Node::class, $reconstructed->subquery);
    }
}
