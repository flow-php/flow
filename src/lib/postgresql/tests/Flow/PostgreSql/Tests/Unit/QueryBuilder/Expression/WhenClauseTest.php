<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\CaseWhen;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Expression\WhenClause;
use PHPUnit\Framework\TestCase;

final class WhenClauseTest extends TestCase
{
    public function test_converts_to_ast(): void
    {
        $condition = Literal::bool(true);
        $result = Literal::int(1);

        $whenClause = new WhenClause($condition, $result);
        $node = $whenClause->toAst();

        static::assertNotNull($node->getCaseWhen());

        $caseWhen = $node->getCaseWhen();
        static::assertNotNull($caseWhen->getExpr());
        static::assertNotNull($caseWhen->getResult());
    }

    public function test_creates_when_clause(): void
    {
        $condition = Literal::bool(true);
        $result = Literal::string('yes');

        $whenClause = new WhenClause($condition, $result);

        static::assertSame($condition, $whenClause->getCondition());
        static::assertSame($result, $whenClause->getResult());
    }

    public function test_recreates_from_ast(): void
    {
        $condition = Literal::int(1);
        $result = Literal::string('one');

        $caseWhen = new CaseWhen();
        $caseWhen->setExpr($condition->toAst());
        $caseWhen->setResult($result->toAst());

        $node = new Node();
        $node->setCaseWhen($caseWhen);

        $whenClause = WhenClause::fromAst($node);

        static::assertInstanceOf(WhenClause::class, $whenClause);
    }

    public function test_round_trip_conversion(): void
    {
        $condition = Literal::bool(false);
        $result = Literal::int(0);

        $whenClause = new WhenClause($condition, $result);
        $node = $whenClause->toAst();
        $restored = WhenClause::fromAst($node);

        static::assertInstanceOf(WhenClause::class, $restored);
    }
}
