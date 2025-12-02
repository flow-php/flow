<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\AST\Transformers;

use Flow\PgQuery\AST\Transformers\{ExistingLimitBehavior, PaginationConfig, PaginationModifier};
use Flow\PgQuery\AST\Traverser;
use Flow\PgQuery\Exception\PaginationException;
use Flow\PgQuery\Parser;
use PHPUnit\Framework\TestCase;

final class PaginationModifierTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_add_pagination_to_simple_select() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users');

        $config = new PaginationConfig(10, 5);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users LIMIT 10 OFFSET 5', $parsed->deparse());
    }

    public function test_add_pagination_without_offset() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT id, name FROM users WHERE active = true');

        $config = new PaginationConfig(20);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT id, name FROM users WHERE active = true LIMIT 20', $parsed->deparse());
    }

    public function test_combine_minimum_with_existing_limit() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users LIMIT 100 OFFSET 10');

        $config = new PaginationConfig(50, 5, ExistingLimitBehavior::COMBINE_MINIMUM);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users LIMIT 50 OFFSET 15', $parsed->deparse());
    }

    public function test_cte_main_query_gets_paginated() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active');

        $config = new PaginationConfig(10);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active LIMIT 10', $parsed->deparse());
    }

    public function test_distinct_query() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT DISTINCT name FROM users');

        $config = new PaginationConfig(10);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT DISTINCT name FROM users LIMIT 10', $parsed->deparse());
    }

    public function test_error_if_limit_exists() : void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Query already contains LIMIT clause');

        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users LIMIT 100');

        $config = new PaginationConfig(10, 0, ExistingLimitBehavior::ERROR_IF_EXISTS);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());
    }

    public function test_except_query_wraps_in_subquery() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT id FROM users EXCEPT SELECT id FROM banned');

        $config = new PaginationConfig(15);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM (SELECT id FROM users EXCEPT SELECT id FROM banned) _pagination_subq LIMIT 15', $parsed->deparse());
    }

    public function test_group_by_having_query() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept HAVING COUNT(*) > 5');

        $config = new PaginationConfig(10);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT dept, count(*) AS cnt FROM users GROUP BY dept HAVING count(*) > 5 LIMIT 10', $parsed->deparse());
    }

    public function test_intersect_query_wraps_in_subquery() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT id FROM users INTERSECT SELECT id FROM admins');

        $config = new PaginationConfig(5, 2);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM (SELECT id FROM users INTERSECT SELECT id FROM admins) _pagination_subq LIMIT 5 OFFSET 2', $parsed->deparse());
    }

    public function test_order_by_query() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY name ASC');

        $config = new PaginationConfig(10);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users ORDER BY name ASC LIMIT 10', $parsed->deparse());
    }

    public function test_override_existing_limit() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users LIMIT 100');

        $config = new PaginationConfig(10, 0, ExistingLimitBehavior::OVERRIDE);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users LIMIT 10', $parsed->deparse());
    }

    public function test_skip_if_limit_exists() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users LIMIT 100');

        $config = new PaginationConfig(10, 0, ExistingLimitBehavior::SKIP_IF_EXISTS);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users LIMIT 100', $parsed->deparse());
    }

    public function test_subquery_in_from_clause_not_paginated() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM (SELECT id FROM users) AS sub');

        $config = new PaginationConfig(10);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM (SELECT id FROM users) sub LIMIT 10', $parsed->deparse());
    }

    public function test_subquery_in_where_clause_not_paginated() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)');

        $config = new PaginationConfig(10);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) LIMIT 10', $parsed->deparse());
    }

    public function test_union_query_wraps_in_subquery() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT id FROM users UNION SELECT id FROM admins');

        $config = new PaginationConfig(10);
        $modifier = new PaginationModifier($config);

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM (SELECT id FROM users UNION SELECT id FROM admins) _pagination_subq LIMIT 10', $parsed->deparse());
    }

    public function test_with_pagination_custom_behavior() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users LIMIT 100');

        $config = new PaginationConfig(10, 0, ExistingLimitBehavior::OVERRIDE);
        $modifier = new PaginationModifier($config);

        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users LIMIT 10', $parsed->deparse());
    }

    public function test_with_pagination_helper_method() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users');

        $config = new PaginationConfig(25, 50);
        $modifier = new PaginationModifier($config);

        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users LIMIT 25 OFFSET 50', $parsed->deparse());
    }
}
