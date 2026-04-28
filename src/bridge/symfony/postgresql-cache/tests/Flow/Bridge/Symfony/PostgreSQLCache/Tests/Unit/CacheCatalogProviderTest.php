<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLCache\CacheCatalogProvider;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use PHPUnit\Framework\TestCase;

final class CacheCatalogProviderTest extends TestCase
{
    public function test_data_column_is_bytea_not_null() : void
    {
        $col = (new CacheCatalogProvider())->get()->get('public')->tables[0]->column('item_data');

        self::assertFalse($col->nullable);
        self::assertTrue($col->type->isEqual(ColumnType::bytea()));
    }

    public function test_get_returns_catalog_with_custom_schema() : void
    {
        $catalog = (new CacheCatalogProvider(schemaName: 'cache'))->get();

        self::assertSame(['cache'], $catalog->names());
    }

    public function test_id_column_is_primary_key_and_varchar_255() : void
    {
        $table = (new CacheCatalogProvider())->get()->get('public')->tables[0];

        self::assertSame(['item_id'], $table->primaryKey?->columns);
        self::assertTrue($table->column('item_id')->type->isEqual(ColumnType::varchar(255)));
        self::assertFalse($table->column('item_id')->nullable);
    }

    public function test_index_on_lifetime_time_uses_table_prefix() : void
    {
        $indexes = (new CacheCatalogProvider('my_cache'))->get()->get('public')->tables[0]->indexes;

        self::assertCount(1, $indexes);
        self::assertSame('idx_my_cache_lifetime_time', $indexes[0]->name);
        self::assertSame(['item_lifetime', 'item_time'], $indexes[0]->columns);
    }

    public function test_lifetime_column_is_integer_nullable() : void
    {
        $col = (new CacheCatalogProvider())->get()->get('public')->tables[0]->column('item_lifetime');

        self::assertTrue($col->nullable);
        self::assertTrue($col->type->isEqual(ColumnType::integer()));
    }

    public function test_table_has_custom_columns() : void
    {
        $table = (new CacheCatalogProvider(
            idCol: 'cid',
            dataCol: 'cdata',
            lifetimeCol: 'cttl',
            timeCol: 'cts',
        ))->get()->get('public')->tables[0];

        self::assertSame(['cid', 'cdata', 'cttl', 'cts'], $table->columnNames());
        self::assertSame(['cid'], $table->primaryKey?->columns);
    }

    public function test_table_has_custom_name_and_schema() : void
    {
        $table = (new CacheCatalogProvider('app_cache', 'app'))->get()->get('app')->tables[0];

        self::assertSame('app_cache', $table->name);
        self::assertSame('app', $table->schema);
    }

    public function test_table_has_default_name() : void
    {
        $table = (new CacheCatalogProvider())->get()->get('public')->tables[0];

        self::assertSame('cache_items', $table->name);
    }

    public function test_time_column_is_integer_not_null() : void
    {
        $col = (new CacheCatalogProvider())->get()->get('public')->tables[0]->column('item_time');

        self::assertFalse($col->nullable);
        self::assertTrue($col->type->isEqual(ColumnType::integer()));
    }
}
