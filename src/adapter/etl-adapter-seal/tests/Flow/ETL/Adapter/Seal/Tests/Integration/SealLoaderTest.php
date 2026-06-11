<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration;

use Flow\ETL\Adapter\Seal\Tests\IntegrationTestCase;

use function Flow\ETL\Adapter\Seal\to_seal_delete;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\Adapter\Seal\to_seal_upsert;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_entry;

final class SealLoaderTest extends IntegrationTestCase
{
    public function test_deleting_documents_by_identifier(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name')),
            'users',
            'id',
        ));

        to_seal_upsert($engine, 'users')->load(
            rows(
                row(string_entry('id', '1'), string_entry('name', 'Alice')),
                row(string_entry('id', '2'), string_entry('name', 'Bob')),
            ),
            flow_context(),
        );
        $this->sealContext()->refresh();

        to_seal_delete($engine, 'users')->load(rows(row(string_entry('id', '1'))), flow_context());
        $this->sealContext()->refresh();

        static::assertSame(1, $engine->countDocuments('users'));
        static::assertSame('Bob', $engine->getDocument('users', '2')['name']);
    }

    public function test_deleting_documents_using_a_custom_identifier_entry(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('sku'), str_schema('name')),
            'products',
            'sku',
        ));

        to_seal_upsert($engine, 'products')->load(
            rows(row(string_entry('sku', 'SKU_0001'), string_entry('name', 'Keyboard'))),
            flow_context(),
        );
        $this->sealContext()->refresh();

        to_seal_delete($engine, 'products')
            ->withIdentifierEntry('sku')
            ->load(rows(row(string_entry('sku', 'SKU_0001'))), flow_context());
        $this->sealContext()->refresh();

        static::assertSame(0, $engine->countDocuments('products'));
    }

    public function test_loading_respects_a_custom_bulk_size(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name'), int_schema('age')),
            'users',
            'id',
        ));

        $documents = [];

        for ($i = 1; $i <= 10; $i++) {
            $documents[] = row(
                string_entry('id', (string) $i),
                string_entry('name', 'User ' . $i),
                integer_entry('age', 20 + $i),
            );
        }

        to_seal_upsert($engine, 'users')->withBulkSize(3)->load(rows(...$documents), flow_context());
        $this->sealContext()->refresh();

        static::assertSame(10, $engine->countDocuments('users'));
    }

    public function test_loading_rows_into_a_seal_index(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name'), int_schema('age')),
            'users',
            'id',
        ));

        to_seal_upsert($engine, 'users')->load(
            rows(
                row(string_entry('id', '1'), string_entry('name', 'Alice'), integer_entry('age', 30)),
                row(string_entry('id', '2'), string_entry('name', 'Bob'), integer_entry('age', 25)),
            ),
            flow_context(),
        );
        $this->sealContext()->refresh();

        static::assertSame(2, $engine->countDocuments('users'));
        static::assertSame('Alice', $engine->getDocument('users', '1')['name']);
    }

    public function test_loading_updates_a_document_with_the_same_identifier(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name'), int_schema('age')),
            'users',
            'id',
        ));
        $loader = to_seal_upsert($engine, 'users');

        $loader->load(
            rows(row(string_entry('id', '1'), string_entry('name', 'Alice'), integer_entry('age', 30))),
            flow_context(),
        );
        $loader->load(
            rows(row(string_entry('id', '1'), string_entry('name', 'Alice Updated'), integer_entry('age', 31))),
            flow_context(),
        );
        $this->sealContext()->refresh();

        static::assertSame(1, $engine->countDocuments('users'));
        static::assertSame('Alice Updated', $engine->getDocument('users', '1')['name']);
    }
}
