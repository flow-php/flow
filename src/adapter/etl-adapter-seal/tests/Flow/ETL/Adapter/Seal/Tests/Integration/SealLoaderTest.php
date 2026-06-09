<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration;

use Flow\ETL\Adapter\Seal\Tests\SealTestCase;

use function Flow\ETL\Adapter\Seal\to_seal;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;

final class SealLoaderTest extends SealTestCase
{
    public function test_loading_respects_a_custom_bulk_size(): void
    {
        $engine = $this->sealContext()->engine();

        $documents = [];

        for ($i = 1; $i <= 10; $i++) {
            $documents[] = row(
                string_entry('id', (string) $i),
                string_entry('name', 'User ' . $i),
                integer_entry('age', 20 + $i),
            );
        }

        to_seal($engine, self::INDEX_NAME)->withBulkSize(3)->load(rows(...$documents), flow_context());

        static::assertSame(10, $engine->countDocuments(self::INDEX_NAME));
    }

    public function test_loading_rows_into_a_seal_index(): void
    {
        $engine = $this->sealContext()->engine();

        to_seal($engine, self::INDEX_NAME)->load(
            rows(
                row(string_entry('id', '1'), string_entry('name', 'Alice'), integer_entry('age', 30)),
                row(string_entry('id', '2'), string_entry('name', 'Bob'), integer_entry('age', 25)),
            ),
            flow_context(),
        );

        static::assertSame(2, $engine->countDocuments(self::INDEX_NAME));
        static::assertSame('Alice', $engine->getDocument(self::INDEX_NAME, '1')['name']);
    }

    public function test_loading_updates_a_document_with_the_same_identifier(): void
    {
        $engine = $this->sealContext()->engine();
        $loader = to_seal($engine, self::INDEX_NAME);

        $loader->load(
            rows(row(string_entry('id', '1'), string_entry('name', 'Alice'), integer_entry('age', 30))),
            flow_context(),
        );
        $loader->load(
            rows(row(string_entry('id', '1'), string_entry('name', 'Alice Updated'), integer_entry('age', 31))),
            flow_context(),
        );

        static::assertSame(1, $engine->countDocuments(self::INDEX_NAME));
        static::assertSame('Alice Updated', $engine->getDocument(self::INDEX_NAME, '1')['name']);
    }
}
