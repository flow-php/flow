<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\ReturningOptionKind;
use Flow\PostgreSql\QueryBuilder\Clause\ReturningClause;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidBuilderStateException;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\literal;

final class ReturningClauseTest extends TestCase
{
    public function test_aliases_without_alias_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('returningAliases() needs an old or a new alias');

        (new ReturningClause())->aliases(null, null);
    }

    public function test_aliases_without_expressions_throw_at_to_ast(): void
    {
        $this->expectException(InvalidBuilderStateException::class);
        $this->expectExceptionMessage(
            'RETURNING WITH (OLD AS o, NEW AS n) needs a RETURNING list. Call returning() or returningAll() after returningAliases().',
        );

        (new ReturningClause())
            ->aliases('o', 'n')
            ->toAst();
    }

    public function test_from_ast_of_another_node_throws(): void
    {
        $this->expectException(InvalidAstException::class);

        ReturningClause::fromAst(literal(1)->toAst());
    }

    public function test_from_ast_reads_expressions_and_aliases(): void
    {
        $clause = (new ReturningClause())
            ->aliases('o', 'n')
            ->returning(col('id', 'o'), col('id', 'n'));

        static::assertEquals($clause, ReturningClause::fromAst($clause->toAst()));
    }

    public function test_returning_without_expressions_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'returning() needs at least one expression. Call returningAll() for RETURNING *.',
        );

        (new ReturningClause())->returning();
    }

    public function test_to_ast_maps_expressions_and_aliases(): void
    {
        $returningClause = (new ReturningClause())
            ->aliases('o', 'n')
            ->returning(col('id', 'o'), col('id', 'n'))
            ->toAst()
            ->getReturningClause();

        static::assertNotNull($returningClause);
        static::assertCount(2, $returningClause->getExprs());
        static::assertSame(
            col('id', 'o')->toAst()->serializeToString(),
            $returningClause->getExprs()[0]->getResTarget()?->getVal()?->serializeToString(),
        );
        static::assertCount(2, $returningClause->getOptions());
        static::assertSame(
            ReturningOptionKind::RETURNING_OPTION_OLD,
            $returningClause->getOptions()[0]->getReturningOption()?->getOption(),
        );
        static::assertSame('o', $returningClause->getOptions()[0]->getReturningOption()?->getValue());
        static::assertSame(
            ReturningOptionKind::RETURNING_OPTION_NEW,
            $returningClause->getOptions()[1]->getReturningOption()?->getOption(),
        );
        static::assertSame('n', $returningClause->getOptions()[1]->getReturningOption()?->getValue());
    }

    public function test_to_ast_without_aliases_has_no_options(): void
    {
        $returningClause = (new ReturningClause())
            ->returning(col('id'))
            ->toAst()
            ->getReturningClause();

        static::assertNotNull($returningClause);
        static::assertCount(1, $returningClause->getExprs());
        static::assertCount(0, $returningClause->getOptions());
    }
}
