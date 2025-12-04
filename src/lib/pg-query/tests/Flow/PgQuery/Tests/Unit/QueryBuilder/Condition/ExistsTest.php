<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Condition;

use Flow\PgQuery\Protobuf\AST\{Node, SelectStmt, SubLinkType};
use Flow\PgQuery\QueryBuilder\Condition\{AndCondition, Exists, NotCondition, OrCondition};
use PHPUnit\Framework\TestCase;

final class ExistsTest extends TestCase
{
    public function test_and_method_returns_and_condition() : void
    {
        $subquery1 = new Node(['select_stmt' => new SelectStmt()]);
        $subquery2 = new Node(['select_stmt' => new SelectStmt()]);

        $condition1 = new Exists($subquery1);
        $condition2 = new Exists($subquery2);

        $result = $condition1->and($condition2);

        self::assertInstanceOf(AndCondition::class, $result);
    }

    public function test_converts_to_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $condition = new Exists($subquery);

        $node = $condition->toAst();

        self::assertTrue($node->hasSubLink());

        $subLink = $node->getSubLink();
        self::assertNotNull($subLink);
        self::assertSame(SubLinkType::EXISTS_SUBLINK, $subLink->getSubLinkType());
        self::assertTrue($subLink->hasSubselect());
    }

    public function test_creates_exists_condition() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);

        $condition = new Exists($subquery);

        self::assertInstanceOf(Exists::class, $condition);
        self::assertSame($subquery, $condition->subquery);
    }

    public function test_not_method_returns_not_condition() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $condition = new Exists($subquery);

        $result = $condition->not();

        self::assertInstanceOf(NotCondition::class, $result);
    }

    public function test_or_method_returns_or_condition() : void
    {
        $subquery1 = new Node(['select_stmt' => new SelectStmt()]);
        $subquery2 = new Node(['select_stmt' => new SelectStmt()]);

        $condition1 = new Exists($subquery1);
        $condition2 = new Exists($subquery2);

        $result = $condition1->or($condition2);

        self::assertInstanceOf(OrCondition::class, $result);
    }

    public function test_reconstructs_from_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $original = new Exists($subquery);

        $node = $original->toAst();
        $reconstructed = Exists::fromAst($node);

        self::assertInstanceOf(Exists::class, $reconstructed);
        self::assertInstanceOf(Node::class, $reconstructed->subquery);
    }
}
