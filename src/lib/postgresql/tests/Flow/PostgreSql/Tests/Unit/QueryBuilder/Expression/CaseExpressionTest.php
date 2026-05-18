<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\CaseExpr;
use Flow\PostgreSql\Protobuf\AST\CaseWhen;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\BinaryExpression;
use Flow\PostgreSql\QueryBuilder\Expression\CaseExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Expression\WhenClause;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class CaseExpressionTest extends TestCase
{
    public function test_converts_searched_case_to_ast(): void
    {
        $whenClause = new WhenClause(
            new BinaryExpression(Column::name('status'), '=', Literal::string('active')),
            Literal::string('Active'),
        );

        $caseExpr = new CaseExpression(null, [$whenClause], Literal::string('Inactive'));

        $node = $caseExpr->toAst();

        $case = $node->getCaseExpr();
        static::assertNotNull($case);
        static::assertNull($case->getArg());
        static::assertNotNull($case->getArgs());
        static::assertCount(1, $case->getArgs());
        static::assertNotNull($case->getDefresult());
    }

    public function test_converts_simple_case_to_ast(): void
    {
        $arg = Column::name('status');
        $whenClause = new WhenClause(Literal::int(1), Literal::string('One'));

        $caseExpr = new CaseExpression($arg, [$whenClause]);

        $node = $caseExpr->toAst();

        $case = $node->getCaseExpr();
        static::assertNotNull($case);
        static::assertNotNull($case->getArg());
        static::assertNotNull($case->getArgs());
        static::assertCount(1, $case->getArgs());
        static::assertNull($case->getDefresult());
    }

    public function test_creates_aliased_expression(): void
    {
        $whenClause = new WhenClause(Literal::bool(true), Literal::int(1));
        $caseExpr = new CaseExpression(null, [$whenClause]);

        $aliased = $caseExpr->as('my_case');

        static::assertSame('my_case', $aliased->getAlias());
        static::assertSame($caseExpr, $aliased->getExpression());
    }

    public function test_creates_searched_case_with_else(): void
    {
        $whenClause = new WhenClause(Literal::bool(true), Literal::string('yes'));
        $elseResult = Literal::string('no');

        $caseExpr = new CaseExpression(null, [$whenClause], $elseResult);

        static::assertNull($caseExpr->getArg());
        static::assertCount(1, $caseExpr->getWhenClauses());
        static::assertSame($elseResult, $caseExpr->getElseResult());
    }

    public function test_creates_searched_case_without_else(): void
    {
        $whenClause = new WhenClause(Literal::bool(true), Literal::string('yes'));

        $caseExpr = new CaseExpression(null, [$whenClause]);

        static::assertNull($caseExpr->getArg());
        static::assertCount(1, $caseExpr->getWhenClauses());
        static::assertNull($caseExpr->getElseResult());
    }

    public function test_creates_simple_case(): void
    {
        $arg = Column::name('value');
        $whenClause = new WhenClause(Literal::int(1), Literal::string('one'));

        $caseExpr = new CaseExpression($arg, [$whenClause]);

        static::assertSame($arg, $caseExpr->getArg());
        static::assertCount(1, $caseExpr->getWhenClauses());
        static::assertNull($caseExpr->getElseResult());
    }

    public function test_recreates_from_ast_with_multiple_when_clauses(): void
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

        static::assertNull($case->getArg());
        static::assertCount(2, $case->getWhenClauses());
        static::assertNotNull($case->getElseResult());
    }

    public function test_recreates_from_ast_with_simple_case(): void
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

        static::assertNotNull($case->getArg());
        static::assertCount(1, $case->getWhenClauses());
    }

    public function test_round_trip_conversion_with_searched_case(): void
    {
        $whenClause = new WhenClause(Literal::bool(true), Literal::int(1));

        $caseExpr = new CaseExpression(null, [$whenClause], Literal::int(0));
        $node = $caseExpr->toAst();
        $restored = CaseExpression::fromAst($node);

        static::assertNull($restored->getArg());
        static::assertCount(1, $restored->getWhenClauses());
        static::assertNotNull($restored->getElseResult());
    }

    public function test_round_trip_conversion_with_simple_case(): void
    {
        $arg = Column::name('value');
        $whenClause = new WhenClause(Literal::int(1), Literal::string('one'));

        $caseExpr = new CaseExpression($arg, [$whenClause]);
        $node = $caseExpr->toAst();
        $restored = CaseExpression::fromAst($node);

        static::assertNotNull($restored->getArg());
        static::assertCount(1, $restored->getWhenClauses());
        static::assertNull($restored->getElseResult());
    }

    public function test_throws_exception_for_empty_when_clauses(): void
    {
        $this->expectException(InvalidExpressionException::class);

        (new ReflectionClass(CaseExpression::class))->newInstance(null, []);
    }

    public function test_with_else_creates_new_instance(): void
    {
        $whenClause = new WhenClause(Literal::bool(true), Literal::int(1));
        $caseExpr = new CaseExpression(null, [$whenClause]);

        $newElse = Literal::int(0);
        $newCase = $caseExpr->withElse($newElse);

        static::assertNotSame($caseExpr, $newCase);
        static::assertNull($caseExpr->getElseResult());
        static::assertSame($newElse, $newCase->getElseResult());
    }

    public function test_with_when_creates_new_instance(): void
    {
        $whenClause1 = new WhenClause(Literal::bool(true), Literal::int(1));
        $caseExpr = new CaseExpression(null, [$whenClause1]);

        $whenClause2 = new WhenClause(Literal::bool(false), Literal::int(0));
        $newCase = $caseExpr->withWhen($whenClause2);

        static::assertNotSame($caseExpr, $newCase);
        static::assertCount(1, $caseExpr->getWhenClauses());
        static::assertCount(1, $newCase->getWhenClauses());
    }
}
