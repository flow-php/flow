<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    array_contained_by,
    array_contains,
    array_overlap,
    col,
    json_contained_by,
    json_contains,
    json_exists,
    json_exists_all,
    json_exists_any,
    json_get,
    json_get_text,
    json_path,
    json_path_text,
    literal_string,
    not_regex_imatch,
    not_regex_match,
    raw_expr,
    regex_imatch,
    regex_match,
    select,
    star,
    table,
    text_search_match
};

final class OperatorConditionTest extends PGQueryTestCase
{
    public function test_array_contained_by() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(array_contained_by(col('tags'), raw_expr("ARRAY['sale', 'featured', 'new']")));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM products WHERE tags <@ ARRAY['sale', 'featured', 'new']"
        );
    }

    public function test_array_contains() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(array_contains(col('tags'), raw_expr("ARRAY['sale']")));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM products WHERE tags @> ARRAY['sale']"
        );
    }

    public function test_array_overlap() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(array_overlap(col('tags'), raw_expr("ARRAY['sale', 'featured']")));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM products WHERE tags && ARRAY['sale', 'featured']"
        );
    }

    public function test_json_contained_by() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_contained_by(col('metadata'), literal_string('{"category": "electronics", "price": 100}')));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE metadata <@ \'{"category": "electronics", "price": 100}\''
        );
    }

    public function test_json_contains() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_contains(col('metadata'), literal_string('{"category": "electronics"}')));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE metadata @> \'{"category": "electronics"}\''
        );
    }

    public function test_json_exists() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_exists(col('metadata'), literal_string('category')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM products WHERE metadata ? 'category'"
        );
    }

    public function test_json_exists_all() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_exists_all(col('metadata'), raw_expr("array['category', 'name']")));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM products WHERE metadata ?& ARRAY['category', 'name']"
        );
    }

    public function test_json_exists_any() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(json_exists_any(col('metadata'), raw_expr("array['category', 'name']")));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM products WHERE metadata ?| ARRAY['category', 'name']"
        );
    }

    public function test_json_get() : void
    {
        $query = select()
            ->select(json_get(col('metadata'), literal_string('category'))->as('category'))
            ->from(table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT metadata -> 'category' AS category FROM products"
        );
    }

    public function test_json_get_text() : void
    {
        $query = select()
            ->select(json_get_text(col('metadata'), literal_string('name'))->as('product_name'))
            ->from(table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT metadata ->> 'name' AS product_name FROM products"
        );
    }

    public function test_json_path() : void
    {
        $query = select()
            ->select(json_path(col('metadata'), literal_string('{category,name}'))->as('nested'))
            ->from(table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT metadata #> '{category,name}' AS nested FROM products"
        );
    }

    public function test_json_path_text() : void
    {
        $query = select()
            ->select(json_path_text(col('metadata'), literal_string('{category,name}'))->as('nested_text'))
            ->from(table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT metadata #>> '{category,name}' AS nested_text FROM products"
        );
    }

    public function test_not_regex_imatch() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(not_regex_imatch(col('email'), literal_string('.*@spam\\.com')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE email !~* E'.*@spam\\\\.com'"
        );
    }

    public function test_not_regex_match() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(not_regex_match(col('email'), literal_string('.*@spam\\.com')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE email !~ E'.*@spam\\\\.com'"
        );
    }

    public function test_regex_imatch() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(regex_imatch(col('email'), literal_string('.*@gmail\\.com')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE email ~* E'.*@gmail\\\\.com'"
        );
    }

    public function test_regex_match() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(regex_match(col('email'), literal_string('.*@gmail\\.com')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE email ~ E'.*@gmail\\\\.com'"
        );
    }

    public function test_text_search_match() : void
    {
        $query = select()
            ->select(star())
            ->from(table('documents'))
            ->where(text_search_match(col('content'), raw_expr("to_tsquery('english', 'hello & world')")));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM documents WHERE content @@ to_tsquery('english', 'hello & world')"
        );
    }
}
