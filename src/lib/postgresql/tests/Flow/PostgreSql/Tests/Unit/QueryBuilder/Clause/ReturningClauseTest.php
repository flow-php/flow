<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\ReturningClause;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Star;
use PHPUnit\Framework\TestCase;

final class ReturningClauseTest extends TestCase
{
    public function test_all(): void
    {
        $returning = ReturningClause::all();

        static::assertCount(1, $returning->expressions());
        static::assertInstanceOf(Star::class, $returning->expressions()[0]);
    }

    public function test_columns(): void
    {
        $returning = ReturningClause::columns('id', 'created_at');

        static::assertCount(2, $returning->expressions());
        static::assertInstanceOf(Column::class, $returning->expressions()[0]);
        static::assertInstanceOf(Column::class, $returning->expressions()[1]);
    }

    public function test_single_column(): void
    {
        $returning = ReturningClause::columns('id');

        static::assertCount(1, $returning->expressions());
    }

    public function test_to_ast(): void
    {
        $returning = ReturningClause::columns('id');
        $node = $returning->toAst();
        $resTarget = $node->getResTarget();

        static::assertNotNull($resTarget);
    }

    public function test_to_ast_nodes(): void
    {
        $returning = ReturningClause::columns('id', 'name');
        $nodes = ReturningClause::toAstNodes($returning);

        static::assertCount(2, $nodes);

        foreach ($nodes as $node) {
            static::assertNotNull($node->getResTarget());
        }
    }
}
