<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder;

use Flow\PostgreSql\Protobuf\AST\ClosePortalStmt;
use Flow\PostgreSql\QueryBuilder\AstDeparser;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\listen;

final class AstDeparserTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_a_statement_node_is_deparsed(): void
    {
        static::assertSame('LISTEN my_channel', (new AstDeparser())->deparse(listen('my_channel')->toAst()));
    }

    public function test_a_multi_word_statement_node_is_deparsed(): void
    {
        static::assertSame('CLOSE my_cursor', (new AstDeparser())->deparse(new ClosePortalStmt([
            'portalname' => 'my_cursor',
        ])));
    }
}
