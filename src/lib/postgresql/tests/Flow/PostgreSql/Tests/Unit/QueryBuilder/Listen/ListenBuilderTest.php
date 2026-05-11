<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Listen;

use Flow\PostgreSql\Protobuf\AST\ListenStmt;
use Flow\PostgreSql\QueryBuilder\Listen\ListenBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\listen;

final class ListenBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_ast_contains_channel_name(): void
    {
        $ast = ListenBuilder::create('my_channel')->toAst();

        static::assertInstanceOf(ListenStmt::class, $ast);
        static::assertSame('my_channel', $ast->getConditionname());
    }

    public function test_deparses_channel_with_dots(): void
    {
        static::assertSame('LISTEN "schema.channel"', listen('schema.channel')->toSql());
    }

    public function test_deparses_channel_with_reserved_keyword(): void
    {
        static::assertSame('LISTEN "select"', listen('select')->toSql());
    }

    public function test_deparses_channel_with_spaces_as_quoted_identifier(): void
    {
        static::assertSame('LISTEN "weird name"', listen('weird name')->toSql());
    }

    public function test_deparses_simple_channel(): void
    {
        static::assertSame('LISTEN my_channel', listen('my_channel')->toSql());
    }

    public function test_dsl_function_returns_final_step(): void
    {
        static::assertSame('LISTEN flow', listen('flow')->toSql());
    }
}
