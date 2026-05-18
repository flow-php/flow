<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Unlisten;

use Flow\PostgreSql\Protobuf\AST\UnlistenStmt;
use Flow\PostgreSql\QueryBuilder\Unlisten\UnlistenBuilder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\unlisten;

final class UnlistenBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_ast_contains_channel_name(): void
    {
        $ast = UnlistenBuilder::create('my_channel')->toAst();

        static::assertInstanceOf(UnlistenStmt::class, $ast);
        static::assertSame('my_channel', $ast->getConditionname());
    }

    public function test_deparses_channel_with_spaces_as_quoted_identifier(): void
    {
        static::assertSame('UNLISTEN "weird name"', unlisten('weird name')->toSql());
    }

    public function test_deparses_simple_channel(): void
    {
        static::assertSame('UNLISTEN my_channel', unlisten('my_channel')->toSql());
    }

    /**
     * The protobuf path cannot express the `UNLISTEN *` wildcard — libpg_query
     * treats `conditionname = "*"` as a literal channel name and emits
     * `UNLISTEN "*"` instead. This test pins that behavior so the limitation
     * is documented in code and surfaces in CI if upstream libpg_query ever
     * changes.
     */
    public function test_wildcard_is_treated_as_literal_channel_name(): void
    {
        static::assertSame('UNLISTEN "*"', unlisten('*')->toSql());
    }
}
