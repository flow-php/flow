<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{CaseExpr, CaseWhen, Node};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\{BinaryExpression, CaseExpression, Column, Literal, WhenClause};
use PHPUnit\Framework\TestCase;

final class CaseExpressionTest extends TestCase
{
    public function test_converts_searched_case_to_ast() : void
    {
        $whenClause = new WhenClause(
            new BinaryExpression(Column::name('status'), '=', Literal::string('active')),
            Literal::string('Active')
        );

        $caseExpr = new CaseExpression(
            null,
            [$whenClause],
            Literal::string('Inactive')
        );

        $node = $caseExpr->toAst();

        self::assertNotNull($node->getCaseExpr());

        $case = $node->getCaseExpr();
        self::assertNull($case->getArg());
        self::assertNotNull($case->getArgs());
        self::assertCount(1, $case->getArgs());
        self::assertNotNull($case->getDefresult());
    }

    public function test_converts_simple_case_to_ast() : void
    {
        $arg = Column::name('status');
        $whenClause = new WhenClause(
            Literal::int(1),
            Literal::string('One')
        );

        $caseExpr = new CaseExpression($arg, [$whenClause]);

        $node = $caseExpr->toAst();

        self::assertNotNull($node->getCaseExpr());

        $case = $node->getCaseExpr();
        self::assertNotNull($case->getArg());
        self::assertNotNull($case->getArgs());
        self::assertCount(1, $case->getArgs());
        self::assertNull($case->getDefresult());
    }

    public function test_creates_aliased_expression() : void
    {
        $whenClause = new WhenClause(Literal::bool(true), Literal::int(1));
        $caseExpr = new CaseExpression(null, [$whenClause]);

        $aliased = $caseExpr->as('my_case');

        self::assertSame('my_case', $aliased->getAlias());
        self::assertSame($caseExpr, $aliased->getExpression());
    }

    public function test_creates_searched_case_with_else() : void
    {
        $whenClause = new WhenClause(
            Literal::bool(true),
            Literal::string('yes')
        );
        $elseResult = Literal::string('no');

        $caseExpr = new CaseExpression(null, [$whenClause], $elseResult);

        self::assertNull($caseExpr->getArg());
        self::assertCount(1, $caseExpr->getWhenClauses());
        self::assertSame($elseResult, $caseExpr->getElseResult());
    }

    public function test_creates_searched_case_without_else() : void
    {
        $whenClause = new WhenClause(
            Literal::bool(true),
            Literal::string('yes')
        );

        $caseExpr = new CaseExpression(null, [$whenClause]);

        self::assertNull($caseExpr->getArg());
        self::assertCount(1, $caseExpr->getWhenClauses());
        self::assertNull($caseExpr->getElseResult());
    }

    public function test_creates_simple_case() : void
    {
        $arg = Column::name('value');
        $whenClause = new WhenClause(Literal::int(1), Literal::string('one'));

        $caseExpr = new CaseExpression($arg, [$whenClause]);

        self::assertSame($arg, $caseExpr->getArg());
        self::assertCount(1, $caseExpr->getWhenClauses());
        self::assertNull($caseExpr->getElseResult());
    }

    public function test_recreates_from_ast_with_multiple_when_clauses() : void
    {
        $caseWhen1 = new CaseWhen();
        $caseWhen1->setExpr(Literal::int(1)->toAst());
        $caseWhen1->setResult(Literal::string('one')->toAst());

        $caseWhen2 = new CaseWhen();
        $caseWhen2->setExpr(Literal::int(2)->toAst());
        $caseWhen2->setResult(Literal::string('two')->toAst());

        $whenNode1 = new Node();
        $whenNode1->setCaseWhen($caseWhen1);

        $whenNode2 = new Node();
        $whenNode2->setCaseWhen($caseWhen2);

        $caseExpr = new CaseExpr();
        $caseExpr->setArgs([$whenNode1, $whenNode2]);
        $caseExpr->setDefresult(Literal::string('other')->toAst());

        $node = new Node();
        $node->setCaseExpr($caseExpr);

        $case = CaseExpression::fromAst($node);

        self::assertNull($case->getArg());
        self::assertCount(2, $case->getWhenClauses());
        self::assertNotNull($case->getElseResult());
    }

    public function test_recreates_from_ast_with_simple_case() : void
    {
        $arg = Column::name('status');

        $caseWhen = new CaseWhen();
        $caseWhen->setExpr(Literal::int(1)->toAst());
        $caseWhen->setResult(Literal::string('one')->toAst());

        $whenNode = new Node();
        $whenNode->setCaseWhen($caseWhen);

        $caseExpr = new CaseExpr();
        $caseExpr->setArg($arg->toAst());
        $caseExpr->setArgs([$whenNode]);

        $node = new Node();
        $node->setCaseExpr($caseExpr);

        $case = CaseExpression::fromAst($node);

        self::assertNotNull($case->getArg());
        self::assertCount(1, $case->getWhenClauses());
    }

    public function test_round_trip_conversion_with_searched_case() : void
    {
        $whenClause = new WhenClause(
            Literal::bool(true),
            Literal::int(1)
        );

        $caseExpr = new CaseExpression(null, [$whenClause], Literal::int(0));
        $node = $caseExpr->toAst();
        $restored = CaseExpression::fromAst($node);

        self::assertNull($restored->getArg());
        self::assertCount(1, $restored->getWhenClauses());
        self::assertNotNull($restored->getElseResult());
    }

    public function test_round_trip_conversion_with_simple_case() : void
    {
        $arg = Column::name('value');
        $whenClause = new WhenClause(Literal::int(1), Literal::string('one'));

        $caseExpr = new CaseExpression($arg, [$whenClause]);
        $node = $caseExpr->toAst();
        $restored = CaseExpression::fromAst($node);

        self::assertNotNull($restored->getArg());
        self::assertCount(1, $restored->getWhenClauses());
        self::assertNull($restored->getElseResult());
    }

    public function test_throws_exception_for_empty_when_clauses() : void
    {
        $this->expectException(InvalidExpressionException::class);

        /** @phpstan-ignore argument.type (intentionally testing exception) */
        new CaseExpression(null, []);
    }

    public function test_with_else_creates_new_instance() : void
    {
        $whenClause = new WhenClause(Literal::bool(true), Literal::int(1));
        $caseExpr = new CaseExpression(null, [$whenClause]);

        $newElse = Literal::int(0);
        $newCase = $caseExpr->withElse($newElse);

        self::assertNotSame($caseExpr, $newCase);
        self::assertNull($caseExpr->getElseResult());
        self::assertSame($newElse, $newCase->getElseResult());
    }

    public function test_with_when_creates_new_instance() : void
    {
        $whenClause1 = new WhenClause(Literal::bool(true), Literal::int(1));
        $caseExpr = new CaseExpression(null, [$whenClause1]);

        $whenClause2 = new WhenClause(Literal::bool(false), Literal::int(0));
        $newCase = $caseExpr->withWhen($whenClause2);

        self::assertNotSame($caseExpr, $newCase);
        self::assertCount(1, $caseExpr->getWhenClauses());
        self::assertCount(1, $newCase->getWhenClauses());
    }
}
