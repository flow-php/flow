<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests;

use function Flow\ETL\Adapter\Seal\to_seal;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;

abstract class AbstractSealLoaderTestCase extends AbstractSealTestCase
{
    public function test_loading_respects_a_custom_bulk_size(): void
    {
        $documents = [];

        for ($i = 1; $i <= 10; $i++) {
            $documents[] = row(
                string_entry('id', (string) $i),
                string_entry('name', 'User ' . $i),
                integer_entry('age', 20 + $i),
            );
        }

        to_seal($this->engine, self::INDEX_NAME)->withBulkSize(3)->load(rows(...$documents), flow_context());

        $this->refresh();

        static::assertSame(10, $this->engine->countDocuments(self::INDEX_NAME));
    }

    public function test_loading_rows_into_a_seal_index(): void
    {
        to_seal($this->engine, self::INDEX_NAME)->load(
            rows(
                row(string_entry('id', '1'), string_entry('name', 'Alice'), integer_entry('age', 30)),
                row(string_entry('id', '2'), string_entry('name', 'Bob'), integer_entry('age', 25)),
            ),
            flow_context(),
        );

        $this->refresh();

        static::assertSame(2, $this->engine->countDocuments(self::INDEX_NAME));
        static::assertSame('Alice', $this->engine->getDocument(self::INDEX_NAME, '1')['name']);
    }

    public function test_loading_updates_a_document_with_the_same_identifier(): void
    {
        $loader = to_seal($this->engine, self::INDEX_NAME);

        $loader->load(
            rows(row(string_entry('id', '1'), string_entry('name', 'Alice'), integer_entry('age', 30))),
            flow_context(),
        );
        $this->refresh();

        $loader->load(
            rows(row(string_entry('id', '1'), string_entry('name', 'Alice Updated'), integer_entry('age', 31))),
            flow_context(),
        );
        $this->refresh();

        static::assertSame(1, $this->engine->countDocuments(self::INDEX_NAME));
        static::assertSame('Alice Updated', $this->engine->getDocument(self::INDEX_NAME, '1')['name']);
    }
}
