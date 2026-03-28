<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use function Flow\PostgreSql\DSL\{
    array_contained_by,
    array_contains,
    array_expr,
    array_overlap,
    col,
    func,
    json_contained_by,
    json_contains,
    json_exists,
    json_exists_all,
    json_exists_any,
    json_get,
    json_get_text,
    json_path,
    json_path_text,
    literal,
    not_regex_imatch,
    not_regex_match,
    regex_imatch,
    regex_match,
    select,
    star,
    table,
    text_search_match
};

use PHPUnit\Framework\TestCase;

final class OperatorConditionTest extends TestCase
{
    public function test_array_contained_by() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(array_contained_by(col('tags'), array_expr([literal('sale'), literal('featured'), literal('new')])));

        self::assertSame(
            "SELECT * FROM products WHERE tags <@ ARRAY['sale', 'featured', 'new']",
            $query->toSql()
        );
    }

    public function test_array_contains() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(array_contains(col('tags'), array_expr([literal('sale')])));

        self::assertSame(
            "SELECT * FROM products WHERE tags @> ARRAY['sale']",
            $query->toSql()
        );
    }

    public function test_array_overlap() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(array_overlap(col('tags'), array_expr([literal('sale'), literal('featured')])));

        self::assertSame(
            "SELECT * FROM products WHERE tags && ARRAY['sale', 'featured']",
            $query->toSql()
        );
    }

    public function test_json_contained_by() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_contained_by(col('metadata'), literal('{"category": "electronics", "price": 100}')));

        self::assertSame(
            'SELECT * FROM products WHERE metadata <@ \'{"category": "electronics", "price": 100}\'',
            $query->toSql()
        );
    }

    public function test_json_contains() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_contains(col('metadata'), literal('{"category": "electronics"}')));

        self::assertSame(
            'SELECT * FROM products WHERE metadata @> \'{"category": "electronics"}\'',
            $query->toSql()
        );
    }

    public function test_json_exists() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_exists(col('metadata'), literal('category')));

        self::assertSame(
            "SELECT * FROM products WHERE metadata ? 'category'",
            $query->toSql()
        );
    }

    public function test_json_exists_all() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_exists_all(col('metadata'), array_expr([literal('category'), literal('name')])));

        self::assertSame(
            "SELECT * FROM products WHERE metadata ?& ARRAY['category', 'name']",
            $query->toSql()
        );
    }

    public function test_json_exists_any() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_exists_any(col('metadata'), array_expr([literal('category'), literal('name')])));

        self::assertSame(
            "SELECT * FROM products WHERE metadata ?| ARRAY['category', 'name']",
            $query->toSql()
        );
    }

    public function test_json_get() : void
    {
        $query = select()
            ->select(json_get(col('metadata'), literal('category'))->as('category'))
            ->from(table('products'));

        self::assertSame(
            "SELECT metadata -> 'category' AS category FROM products",
            $query->toSql()
        );
    }

    public function test_json_get_text() : void
    {
        $query = select()
            ->select(json_get_text(col('metadata'), literal('name'))->as('product_name'))
            ->from(table('products'));

        self::assertSame(
            "SELECT metadata ->> 'name' AS product_name FROM products",
            $query->toSql()
        );
    }

    public function test_json_path() : void
    {
        $query = select()
            ->select(json_path(col('metadata'), literal('{category,name}'))->as('nested'))
            ->from(table('products'));

        self::assertSame(
            "SELECT metadata #> '{category,name}' AS nested FROM products",
            $query->toSql()
        );
    }

    public function test_json_path_text() : void
    {
        $query = select()
            ->select(json_path_text(col('metadata'), literal('{category,name}'))->as('nested_text'))
            ->from(table('products'));

        self::assertSame(
            "SELECT metadata #>> '{category,name}' AS nested_text FROM products",
            $query->toSql()
        );
    }

    public function test_not_regex_imatch() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(not_regex_imatch(col('email'), literal('.*@spam\\.com')));

        self::assertSame(
            "SELECT * FROM users WHERE email !~* E'.*@spam\\\\.com'",
            $query->toSql()
        );
    }

    public function test_not_regex_match() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(not_regex_match(col('email'), literal('.*@spam\\.com')));

        self::assertSame(
            "SELECT * FROM users WHERE email !~ E'.*@spam\\\\.com'",
            $query->toSql()
        );
    }

    public function test_regex_imatch() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(regex_imatch(col('email'), literal('.*@gmail\\.com')));

        self::assertSame(
            "SELECT * FROM users WHERE email ~* E'.*@gmail\\\\.com'",
            $query->toSql()
        );
    }

    public function test_regex_match() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(regex_match(col('email'), literal('.*@gmail\\.com')));

        self::assertSame(
            "SELECT * FROM users WHERE email ~ E'.*@gmail\\\\.com'",
            $query->toSql()
        );
    }

    public function test_text_search_match() : void
    {
        $query = select()
            ->select(star())
            ->from(table('documents'))
            ->where(text_search_match(col('content'), func('to_tsquery', [literal('english'), literal('hello & world')])));

        self::assertSame(
            "SELECT * FROM documents WHERE content @@ to_tsquery('english', 'hello & world')",
            $query->toSql()
        );
    }
}
