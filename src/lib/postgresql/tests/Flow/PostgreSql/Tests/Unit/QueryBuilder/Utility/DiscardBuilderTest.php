<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\DiscardMode;
use Flow\PostgreSql\Protobuf\AST\DiscardStmt;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardType;
use PHPUnit\Framework\TestCase;

final class DiscardBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_discard_all(): void
    {
        $builder = DiscardBuilder::create(DiscardType::ALL);

        $ast = $builder->toAst();

        static::assertInstanceOf(DiscardStmt::class, $ast);
        static::assertSame(DiscardMode::DISCARD_ALL, $ast->getTarget());
    }

    public function test_discard_plans(): void
    {
        $builder = DiscardBuilder::create(DiscardType::PLANS);

        $ast = $builder->toAst();

        static::assertSame(DiscardMode::DISCARD_PLANS, $ast->getTarget());
    }

    public function test_discard_sequences(): void
    {
        $builder = DiscardBuilder::create(DiscardType::SEQUENCES);

        $ast = $builder->toAst();

        static::assertSame(DiscardMode::DISCARD_SEQUENCES, $ast->getTarget());
    }

    public function test_discard_temp(): void
    {
        $builder = DiscardBuilder::create(DiscardType::TEMP);

        $ast = $builder->toAst();

        static::assertSame(DiscardMode::DISCARD_TEMP, $ast->getTarget());
    }
}
