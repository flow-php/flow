<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\schema;

use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\Exception\SchemaException;
use PHPUnit\Framework\TestCase;

final class CatalogTest extends TestCase
{
    public function test_all_returns_all_schemas() : void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
        ]);

        self::assertCount(2, $catalog->all());
        self::assertSame('public', $catalog->all()[0]->name);
        self::assertSame('audit', $catalog->all()[1]->name);
    }

    public function test_empty_catalog() : void
    {
        $catalog = new Catalog([]);

        self::assertSame([], $catalog->all());
        self::assertSame([], $catalog->names());
        self::assertFalse($catalog->has('anything'));
    }

    public function test_get_returns_schema_by_name() : void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
        ]);

        self::assertSame('audit', $catalog->get('audit')->name);
    }

    public function test_get_throws_when_schema_not_found() : void
    {
        $catalog = new Catalog([schema('public')]);

        $this->expectException(SchemaException::class);
        $catalog->get('missing');
    }

    public function test_has_returns_false_for_missing_schema() : void
    {
        $catalog = new Catalog([schema('public')]);

        self::assertFalse($catalog->has('missing'));
    }

    public function test_has_returns_true_for_existing_schema() : void
    {
        $catalog = new Catalog([schema('public')]);

        self::assertTrue($catalog->has('public'));
    }

    public function test_names_returns_all_schema_names() : void
    {
        $catalog = new Catalog([
            schema('public'),
            schema('audit'),
            schema('staging'),
        ]);

        self::assertSame(['public', 'audit', 'staging'], $catalog->names());
    }
}
