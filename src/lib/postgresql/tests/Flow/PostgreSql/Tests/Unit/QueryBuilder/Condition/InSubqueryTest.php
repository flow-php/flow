<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\SubLinkType;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Condition\InSubquery;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\any_;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\exists;
use function Flow\PostgreSql\DSL\in_subquery;
use function Flow\PostgreSql\DSL\not_;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class InSubqueryTest extends TestCase
{
    public function test_from_ast_of_any_sublink_with_operator_throws(): void
    {
        $this->expectException(InvalidAstException::class);

        InSubquery::fromAst(any_(col('id'), ComparisonOperator::EQ, select(col('id'))->from(table('d')))->toAst());
    }

    public function test_from_ast_of_another_sublink_throws(): void
    {
        $this->expectException(InvalidAstException::class);

        InSubquery::fromAst(exists(select(col('id'))->from(table('d')))->toAst());
    }

    public function test_from_ast_reads_to_ast_node(): void
    {
        $condition = in_subquery('id', select(col('id'))->from(table('d')));

        static::assertEquals($condition, InSubquery::fromAst($condition->toAst()));
    }

    public function test_in_subquery_to_sql(): void
    {
        static::assertSame(
            'SELECT id FROM parents WHERE id IN (SELECT id FROM d)',
            select(col('id'))
                ->from(table('parents'))
                ->where(in_subquery('id', select(col('id'))->from(table('d'))))
                ->toSql(),
        );
    }

    public function test_not_in_subquery_to_sql(): void
    {
        static::assertSame(
            'SELECT id FROM parents WHERE NOT id IN (SELECT id FROM d)',
            select(col('id'))
                ->from(table('parents'))
                ->where(not_(in_subquery('id', select(col('id'))->from(table('d')))))
                ->toSql(),
        );
    }

    public function test_to_ast_writes_any_sublink_without_operator(): void
    {
        $subLink = in_subquery('id', select(col('id'))->from(table('d')))->toAst()->getSubLink();

        static::assertNotNull($subLink);
        static::assertSame(SubLinkType::ANY_SUBLINK, $subLink->getSubLinkType());
        static::assertCount(0, $subLink->getOperName());
    }
}
