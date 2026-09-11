<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Unit;

use Flow\ETL\Adapter\Seal\Tests\SealTestCase;
use Flow\ETL\Exception\RuntimeException;

use function Flow\ETL\Adapter\Seal\to_seal_delete;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\Adapter\Seal\to_seal_upsert;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class SealLoaderTest extends SealTestCase
{
    public function test_deleting_with_a_non_scalar_identifier_entry_throws(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(schema(str_schema('id')), 'users', 'id'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Entry "id" cannot be used as a document identifier for DELETE operation');

        to_seal_delete($engine, 'users')->load(
            rows(schema(list_schema('id', type_list(type_string()))), row(['id' => ['1', '2']])),
            flow_context(),
        );
    }

    public function test_deleting_empty_rows_removes_no_documents(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(schema(str_schema('id')), 'users', 'id'));

        to_seal_upsert($engine, 'users')->load(rows(schema(string_schema('id')), row(['id' => '1'])), flow_context());
        to_seal_delete($engine, 'users')->load(rows(schema()), flow_context());

        static::assertSame(1, $engine->countDocuments('users'));
    }

    public function test_loading_empty_rows_writes_no_documents(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(schema(str_schema('id')), 'users', 'id'));

        to_seal_upsert($engine, 'users')->load(rows(schema()), flow_context());

        static::assertSame(0, $engine->countDocuments('users'));
    }

    public function test_with_bulk_size_returns_the_same_loader_instance(): void
    {
        $loader = to_seal_upsert(
            $this->sealContext()->engine(to_seal_schema(schema(str_schema('id')), 'users', 'id')),
            'users',
        );

        static::assertSame($loader, $loader->withBulkSize(50));
    }

    public function test_with_identifier_entry_returns_the_same_loader_instance(): void
    {
        $loader = to_seal_delete(
            $this->sealContext()->engine(to_seal_schema(schema(str_schema('id')), 'users', 'id')),
            'users',
        );

        static::assertSame($loader, $loader->withIdentifierEntry('sku'));
    }
}
