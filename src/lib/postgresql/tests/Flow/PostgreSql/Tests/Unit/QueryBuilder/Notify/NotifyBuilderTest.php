<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Notify;

use function Flow\PostgreSql\DSL\notify;
use Flow\PostgreSql\Protobuf\AST\NotifyStmt;
use Flow\PostgreSql\QueryBuilder\Notify\NotifyBuilder;

use PHPUnit\Framework\TestCase;

final class NotifyBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_ast_contains_channel_name() : void
    {
        $ast = NotifyBuilder::create('my_channel')->toAst();

        self::assertInstanceOf(NotifyStmt::class, $ast);
        self::assertSame('my_channel', $ast->getConditionname());
        self::assertSame('', $ast->getPayload());
    }

    public function test_ast_contains_payload_when_set() : void
    {
        $ast = NotifyBuilder::create('my_channel')->withPayload('hello')->toAst();

        self::assertSame('my_channel', $ast->getConditionname());
        self::assertSame('hello', $ast->getPayload());
    }

    public function test_deparses_channel_with_spaces_as_quoted_identifier() : void
    {
        self::assertSame('NOTIFY "weird name"', notify('weird name')->toSql());
    }

    public function test_deparses_simple_channel_without_payload() : void
    {
        self::assertSame('NOTIFY my_channel', notify('my_channel')->toSql());
    }

    public function test_deparses_with_payload() : void
    {
        self::assertSame(
            "NOTIFY my_channel, 'hello'",
            notify('my_channel')->withPayload('hello')->toSql(),
        );
    }

    public function test_empty_payload_is_treated_as_no_payload() : void
    {
        self::assertSame('NOTIFY my_channel', notify('my_channel')->withPayload('')->toSql());
    }

    public function test_immutability_of_with_payload() : void
    {
        $original = NotifyBuilder::create('my_channel');
        $modified = $original->withPayload('hello');

        self::assertSame('NOTIFY my_channel', $original->toSql());
        self::assertSame("NOTIFY my_channel, 'hello'", $modified->toSql());
    }

    public function test_payload_with_single_quote_is_escaped() : void
    {
        self::assertSame(
            "NOTIFY my_channel, 'it''s fine'",
            notify('my_channel')->withPayload("it's fine")->toSql(),
        );
    }
}
