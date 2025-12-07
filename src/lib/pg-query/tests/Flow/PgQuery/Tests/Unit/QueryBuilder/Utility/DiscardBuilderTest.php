<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\{DiscardMode, DiscardStmt};
use Flow\PgQuery\QueryBuilder\Utility\{DiscardBuilder, DiscardType};
use PHPUnit\Framework\TestCase;

final class DiscardBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_discard_all() : void
    {
        $builder = DiscardBuilder::create(DiscardType::ALL);

        $ast = $builder->toAst();

        self::assertInstanceOf(DiscardStmt::class, $ast);
        self::assertSame(DiscardMode::DISCARD_ALL, $ast->getTarget());
    }

    public function test_discard_plans() : void
    {
        $builder = DiscardBuilder::create(DiscardType::PLANS);

        $ast = $builder->toAst();

        self::assertSame(DiscardMode::DISCARD_PLANS, $ast->getTarget());
    }

    public function test_discard_sequences() : void
    {
        $builder = DiscardBuilder::create(DiscardType::SEQUENCES);

        $ast = $builder->toAst();

        self::assertSame(DiscardMode::DISCARD_SEQUENCES, $ast->getTarget());
    }

    public function test_discard_temp() : void
    {
        $builder = DiscardBuilder::create(DiscardType::TEMP);

        $ast = $builder->toAst();

        self::assertSame(DiscardMode::DISCARD_TEMP, $ast->getTarget());
    }
}
