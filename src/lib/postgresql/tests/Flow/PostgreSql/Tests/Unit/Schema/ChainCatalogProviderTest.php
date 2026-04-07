<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\{chain_catalog_provider, manual_catalog_provider, schema, schema_column_integer, schema_column_text, schema_column_varchar, schema_table};

use Flow\PostgreSql\Schema\{Catalog, ChainCatalogProvider};
use PHPUnit\Framework\TestCase;

final class ChainCatalogProviderTest extends TestCase
{
    public function test_empty_chain_returns_empty_catalog() : void
    {
        $provider = new ChainCatalogProvider();

        self::assertSame([], $provider->get()->all());
    }

    public function test_single_provider_returns_its_catalog_unchanged() : void
    {
        $catalog = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);

        $provider = chain_catalog_provider(manual_catalog_provider($catalog));

        $result = $provider->get();
        self::assertSame(['public'], $result->names());
        self::assertTrue($result->get('public')->hasTable('users'));
    }

    public function test_three_providers_merged_in_order() : void
    {
        $first = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);
        $second = new Catalog([schema('public', tables: [
            schema_table('posts', [schema_column_integer('id', nullable: false)]),
        ])]);
        $third = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_text('name')]),
        ])]);

        $provider = chain_catalog_provider(
            manual_catalog_provider($first),
            manual_catalog_provider($second),
            manual_catalog_provider($third),
        );

        $result = $provider->get();
        self::assertTrue($result->get('public')->hasTable('users'));
        self::assertTrue($result->get('public')->hasTable('posts'));
        self::assertCount(1, $result->get('public')->table('users')->columns);
        self::assertSame('name', $result->get('public')->table('users')->columns[0]->name);
    }

    public function test_two_providers_with_distinct_schemas_are_combined() : void
    {
        $first = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);
        $second = new Catalog([schema('audit', tables: [
            schema_table('logs', [schema_column_integer('id', nullable: false)]),
        ])]);

        $provider = chain_catalog_provider(
            manual_catalog_provider($first),
            manual_catalog_provider($second),
        );

        $result = $provider->get();
        self::assertSame(['public', 'audit'], $result->names());
        self::assertTrue($result->get('public')->hasTable('users'));
        self::assertTrue($result->get('audit')->hasTable('logs'));
    }

    public function test_two_providers_with_overlapping_table_later_wins() : void
    {
        $first = new Catalog([schema('public', tables: [
            schema_table('users', [schema_column_integer('id', nullable: false)]),
        ])]);
        $second = new Catalog([schema('public', tables: [
            schema_table('users', [
                schema_column_integer('id', nullable: false),
                schema_column_varchar('email', 255, nullable: false),
            ]),
        ])]);

        $provider = chain_catalog_provider(
            manual_catalog_provider($first),
            manual_catalog_provider($second),
        );

        $result = $provider->get();
        self::assertCount(2, $result->get('public')->table('users')->columns);
    }
}
