<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{Node, SelectStmt, SubLink, SubLinkType};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\Subquery;
use PHPUnit\Framework\TestCase;

final class SubqueryTest extends TestCase
{
    public function test_converts_to_ast() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $subquery = new Subquery($selectNode);
        $node = $subquery->toAst();

        $subLink = $node->getSubLink();
        self::assertNotNull($subLink);
        self::assertSame(SubLinkType::EXPR_SUBLINK, $subLink->getSubLinkType());
        self::assertNotNull($subLink->getSubselect());
    }

    public function test_creates_aliased_expression() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $subquery = new Subquery($selectNode);
        $aliased = $subquery->as('subq');

        self::assertSame('subq', $aliased->getAlias());
        self::assertSame($subquery, $aliased->getExpression());
    }

    public function test_creates_subquery() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $subquery = new Subquery($selectNode);

        self::assertSame($selectNode, $subquery->getSelectStatement());
    }

    public function test_recreates_from_ast() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $subLink = new SubLink();
        $subLink->setSubLinkType(SubLinkType::EXPR_SUBLINK);
        $subLink->setSubselect($selectNode);

        $node = new Node();
        $node->setSubLink($subLink);

        $subquery = Subquery::fromAst($node);

        self::assertInstanceOf(Subquery::class, $subquery);
        self::assertInstanceOf(Node::class, $subquery->getSelectStatement());
    }

    public function test_round_trip_conversion() : void
    {
        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $subquery = new Subquery($selectNode);
        $node = $subquery->toAst();
        $restored = Subquery::fromAst($node);

        self::assertInstanceOf(Subquery::class, $restored);
    }

    public function test_throws_exception_for_non_expr_sublink_type() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('expected EXPR_SUBLINK');

        $selectStmt = new SelectStmt();
        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $subLink = new SubLink();
        $subLink->setSubLinkType(SubLinkType::EXISTS_SUBLINK);
        $subLink->setSubselect($selectNode);

        $node = new Node();
        $node->setSubLink($subLink);

        Subquery::fromAst($node);
    }
}
