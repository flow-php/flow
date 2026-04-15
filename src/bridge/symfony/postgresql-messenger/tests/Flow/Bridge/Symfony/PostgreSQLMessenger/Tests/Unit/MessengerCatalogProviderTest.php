<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLMessenger\MessengerCatalogProvider;
use Flow\PostgreSql\Schema\IdentityGeneration;
use PHPUnit\Framework\TestCase;

final class MessengerCatalogProviderTest extends TestCase
{
    public function test_body_headers_queue_name_not_nullable() : void
    {
        $provider = new MessengerCatalogProvider();
        $table = $provider->get()->get('public')->tables[0];

        self::assertFalse($table->column('body')->nullable);
        self::assertFalse($table->column('headers')->nullable);
        self::assertFalse($table->column('queue_name')->nullable);
        self::assertFalse($table->column('created_at')->nullable);
        self::assertFalse($table->column('available_at')->nullable);
    }

    public function test_delivered_at_is_nullable() : void
    {
        $provider = new MessengerCatalogProvider();
        $col = $provider->get()->get('public')->tables[0]->column('delivered_at');

        self::assertTrue($col->nullable);
    }

    public function test_get_returns_catalog_with_custom_schema() : void
    {
        $provider = new MessengerCatalogProvider(schemaName: 'messaging');
        $catalog = $provider->get();

        self::assertSame(['messaging'], $catalog->names());
    }

    public function test_get_returns_catalog_with_public_schema_by_default() : void
    {
        $provider = new MessengerCatalogProvider();
        $catalog = $provider->get();

        self::assertSame(['public'], $catalog->names());
    }

    public function test_id_column_is_identity_always() : void
    {
        $provider = new MessengerCatalogProvider();
        $idColumn = $provider->get()->get('public')->tables[0]->column('id');

        self::assertTrue($idColumn->isIdentity);
        self::assertSame(IdentityGeneration::ALWAYS, $idColumn->identityGeneration);
        self::assertFalse($idColumn->nullable);
    }

    public function test_index_names_use_custom_table_name_prefix() : void
    {
        $provider = new MessengerCatalogProvider('my_queue');
        $indexNames = \array_map(
            static fn ($i) => $i->name,
            $provider->get()->get('public')->tables[0]->indexes,
        );

        self::assertContains('idx_my_queue_queue_name', $indexNames);
        self::assertContains('idx_my_queue_available_at', $indexNames);
        self::assertContains('idx_my_queue_delivered_at', $indexNames);
    }

    public function test_queue_name_has_default_value() : void
    {
        $provider = new MessengerCatalogProvider();
        $queueNameColumn = $provider->get()->get('public')->tables[0]->column('queue_name');

        self::assertSame("'default'", $queueNameColumn->default);
    }

    public function test_table_has_custom_name_and_schema() : void
    {
        $provider = new MessengerCatalogProvider('custom_queue', 'app');
        $table = $provider->get()->get('app')->tables[0];

        self::assertSame('custom_queue', $table->name);
        self::assertSame('app', $table->schema);
    }

    public function test_table_has_default_name() : void
    {
        $provider = new MessengerCatalogProvider();
        $table = $provider->get()->get('public')->tables[0];

        self::assertSame('messenger_messages', $table->name);
    }

    public function test_table_has_expected_columns() : void
    {
        $provider = new MessengerCatalogProvider();
        $table = $provider->get()->get('public')->tables[0];

        self::assertSame(
            ['id', 'body', 'headers', 'queue_name', 'created_at', 'available_at', 'delivered_at'],
            $table->columnNames(),
        );
    }

    public function test_table_has_primary_key_on_id() : void
    {
        $provider = new MessengerCatalogProvider();
        $table = $provider->get()->get('public')->tables[0];

        self::assertNotNull($table->primaryKey);
        self::assertSame(['id'], $table->primaryKey->columns);
    }

    public function test_table_has_three_indexes() : void
    {
        $provider = new MessengerCatalogProvider();
        $indexes = $provider->get()->get('public')->tables[0]->indexes;

        self::assertCount(3, $indexes);
        $indexColumns = \array_map(static fn ($i) => $i->columns, $indexes);
        self::assertContains(['queue_name'], $indexColumns);
        self::assertContains(['available_at'], $indexColumns);
        self::assertContains(['delivered_at'], $indexColumns);
    }
}
