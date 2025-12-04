<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Clause;

use Flow\PgQuery\QueryBuilder\Clause\ReturningClause;
use Flow\PgQuery\QueryBuilder\Expression\{Column, Star};
use PHPUnit\Framework\TestCase;

final class ReturningClauseTest extends TestCase
{
    public function test_all() : void
    {
        $returning = ReturningClause::all();

        self::assertCount(1, $returning->expressions());
        self::assertInstanceOf(Star::class, $returning->expressions()[0]);
    }

    public function test_columns() : void
    {
        $returning = ReturningClause::columns('id', 'created_at');

        self::assertCount(2, $returning->expressions());
        self::assertInstanceOf(Column::class, $returning->expressions()[0]);
        self::assertInstanceOf(Column::class, $returning->expressions()[1]);
    }

    public function test_single_column() : void
    {
        $returning = ReturningClause::columns('id');

        self::assertCount(1, $returning->expressions());
    }

    public function test_to_ast() : void
    {
        $returning = ReturningClause::columns('id');
        $node = $returning->toAst();
        $resTarget = $node->getResTarget();

        self::assertNotNull($resTarget);
    }

    public function test_to_ast_nodes() : void
    {
        $returning = ReturningClause::columns('id', 'name');
        $nodes = ReturningClause::toAstNodes($returning);

        self::assertCount(2, $nodes);

        foreach ($nodes as $node) {
            self::assertNotNull($node->getResTarget());
        }
    }
}
