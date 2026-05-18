<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLMessenger\MessengerCatalogProvider;
use Flow\PostgreSql\Schema\IdentityGeneration;
use PHPUnit\Framework\TestCase;

use function array_map;

final class MessengerCatalogProviderTest extends TestCase
{
    public function test_body_headers_queue_name_not_nullable(): void
    {
        $provider = new MessengerCatalogProvider();
        $table = $provider->get()->get('public')->tables[0];

        static::assertFalse($table->column('body')->nullable);
        static::assertFalse($table->column('headers')->nullable);
        static::assertFalse($table->column('queue_name')->nullable);
        static::assertFalse($table->column('created_at')->nullable);
        static::assertFalse($table->column('available_at')->nullable);
    }

    public function test_delivered_at_is_nullable(): void
    {
        $provider = new MessengerCatalogProvider();
        $col = $provider->get()->get('public')->tables[0]->column('delivered_at');

        static::assertTrue($col->nullable);
    }

    public function test_get_returns_catalog_with_custom_schema(): void
    {
        $provider = new MessengerCatalogProvider(schemaName: 'messaging');
        $catalog = $provider->get();

        static::assertSame(['messaging'], $catalog->names());
    }

    public function test_get_returns_catalog_with_public_schema_by_default(): void
    {
        $provider = new MessengerCatalogProvider();
        $catalog = $provider->get();

        static::assertSame(['public'], $catalog->names());
    }

    public function test_id_column_is_identity_always(): void
    {
        $provider = new MessengerCatalogProvider();
        $idColumn = $provider->get()->get('public')->tables[0]->column('id');

        static::assertTrue($idColumn->isIdentity);
        static::assertSame(IdentityGeneration::ALWAYS, $idColumn->identityGeneration);
        static::assertFalse($idColumn->nullable);
    }

    public function test_index_names_use_custom_table_name_prefix(): void
    {
        $provider = new MessengerCatalogProvider('my_queue');
        $indexNames = array_map(static fn($i) => $i->name, $provider->get()->get('public')->tables[0]->indexes);

        static::assertContains('idx_my_queue_queue_name', $indexNames);
        static::assertContains('idx_my_queue_available_at', $indexNames);
        static::assertContains('idx_my_queue_delivered_at', $indexNames);
    }

    public function test_queue_name_has_default_value(): void
    {
        $provider = new MessengerCatalogProvider();
        $queueNameColumn = $provider->get()->get('public')->tables[0]->column('queue_name');

        static::assertSame("'default'", $queueNameColumn->default);
    }

    public function test_table_has_custom_name_and_schema(): void
    {
        $provider = new MessengerCatalogProvider('custom_queue', 'app');
        $table = $provider->get()->get('app')->tables[0];

        static::assertSame('custom_queue', $table->name);
        static::assertSame('app', $table->schema);
    }

    public function test_table_has_default_name(): void
    {
        $provider = new MessengerCatalogProvider();
        $table = $provider->get()->get('public')->tables[0];

        static::assertSame('messenger_messages', $table->name);
    }

    public function test_table_has_expected_columns(): void
    {
        $provider = new MessengerCatalogProvider();
        $table = $provider->get()->get('public')->tables[0];

        static::assertSame(
            ['id', 'body', 'headers', 'queue_name', 'created_at', 'available_at', 'delivered_at'],
            $table->columnNames(),
        );
    }

    public function test_table_has_primary_key_on_id(): void
    {
        $provider = new MessengerCatalogProvider();
        $table = $provider->get()->get('public')->tables[0];

        static::assertNotNull($table->primaryKey);
        static::assertSame(['id'], $table->primaryKey->columns);
    }

    public function test_table_has_three_indexes(): void
    {
        $provider = new MessengerCatalogProvider();
        $indexes = $provider->get()->get('public')->tables[0]->indexes;

        static::assertCount(3, $indexes);
        $indexColumns = array_map(static fn($i) => $i->columns, $indexes);
        static::assertContains(['queue_name'], $indexColumns);
        static::assertContains(['available_at'], $indexColumns);
        static::assertContains(['delivered_at'], $indexColumns);
    }
}
