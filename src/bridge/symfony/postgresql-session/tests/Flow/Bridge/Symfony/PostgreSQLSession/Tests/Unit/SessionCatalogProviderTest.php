<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLSession\SessionCatalogProvider;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use PHPUnit\Framework\TestCase;

final class SessionCatalogProviderTest extends TestCase
{
    public function test_data_column_is_bytea_not_null() : void
    {
        $col = (new SessionCatalogProvider())->get()->get('public')->tables[0]->column('sess_data');

        self::assertFalse($col->nullable);
        self::assertTrue($col->type->isEqual(ColumnType::bytea()));
    }

    public function test_get_returns_catalog_with_custom_schema() : void
    {
        $catalog = (new SessionCatalogProvider(schemaName: 'session'))->get();

        self::assertSame(['session'], $catalog->names());
    }

    public function test_id_column_is_primary_key_and_varchar_128() : void
    {
        $table = (new SessionCatalogProvider())->get()->get('public')->tables[0];

        self::assertSame(['sess_id'], $table->primaryKey?->columns);
        self::assertTrue($table->column('sess_id')->type->isEqual(ColumnType::varchar(128)));
        self::assertFalse($table->column('sess_id')->nullable);
    }

    public function test_index_on_lifetime_uses_table_prefix() : void
    {
        $indexes = (new SessionCatalogProvider('app_sessions'))->get()->get('public')->tables[0]->indexes;

        self::assertCount(1, $indexes);
        self::assertSame('idx_app_sessions_lifetime', $indexes[0]->name);
        self::assertSame(['sess_lifetime'], $indexes[0]->columns);
    }

    public function test_lifetime_column_is_integer_not_null() : void
    {
        $col = (new SessionCatalogProvider())->get()->get('public')->tables[0]->column('sess_lifetime');

        self::assertFalse($col->nullable);
        self::assertTrue($col->type->isEqual(ColumnType::integer()));
    }

    public function test_table_has_custom_columns() : void
    {
        $table = (new SessionCatalogProvider(
            idCol: 'sid',
            dataCol: 'sdata',
            lifetimeCol: 'sttl',
            timeCol: 'sts',
        ))->get()->get('public')->tables[0];

        self::assertSame(['sid', 'sdata', 'sttl', 'sts'], $table->columnNames());
        self::assertSame(['sid'], $table->primaryKey?->columns);
    }

    public function test_table_has_custom_name_and_schema() : void
    {
        $table = (new SessionCatalogProvider('app_sessions', 'app'))->get()->get('app')->tables[0];

        self::assertSame('app_sessions', $table->name);
        self::assertSame('app', $table->schema);
    }

    public function test_table_has_default_name() : void
    {
        $table = (new SessionCatalogProvider())->get()->get('public')->tables[0];

        self::assertSame('sessions', $table->name);
    }

    public function test_time_column_is_integer_not_null() : void
    {
        $col = (new SessionCatalogProvider())->get()->get('public')->tables[0]->column('sess_time');

        self::assertFalse($col->nullable);
        self::assertTrue($col->type->isEqual(ColumnType::integer()));
    }
}
