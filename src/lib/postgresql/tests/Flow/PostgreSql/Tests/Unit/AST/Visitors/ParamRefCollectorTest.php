<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Visitors;

use function Flow\PostgreSql\DSL\sql_parse;
use Flow\PostgreSql\AST\Visitors\ParamRefCollector;
use PHPUnit\Framework\TestCase;

final class ParamRefCollectorTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_collects_multiple_params() : void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE status = $1 AND category = $2 AND created_at > $3');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        self::assertCount(3, $collector->getParamRefs());
        self::assertSame(3, $collector->getMaxParamNumber());
    }

    public function test_collects_no_params_from_query_without_placeholders() : void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE active = true');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        self::assertSame([], $collector->getParamRefs());
        self::assertSame(0, $collector->getMaxParamNumber());
    }

    public function test_collects_single_param() : void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE id = $1');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        self::assertCount(1, $collector->getParamRefs());
        self::assertSame(1, $collector->getMaxParamNumber());
    }

    public function test_finds_max_param_number_with_gaps() : void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE id = $3 AND status = $1');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        self::assertCount(2, $collector->getParamRefs());
        self::assertSame(3, $collector->getMaxParamNumber());
    }

    public function test_reset_clears_collected_params() : void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE id = $1');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        self::assertCount(1, $collector->getParamRefs());

        $collector->reset();

        self::assertSame([], $collector->getParamRefs());
        self::assertSame(0, $collector->getMaxParamNumber());
    }
}
