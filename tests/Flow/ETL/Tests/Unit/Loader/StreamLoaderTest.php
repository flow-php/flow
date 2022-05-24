<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\To;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Stream\Mode;
use PHPUnit\Framework\TestCase;

final class StreamLoaderTest extends TestCase
{
    public function test_loading_data_int_invalid_stream() : void
    {
        $this->expectExceptionMessage("Can't open stream for url: php://qweqweqw in mode: w. Reason: fopen(): Invalid php:// URL specified");
        $this->expectException(RuntimeException::class);

        $loader = To::stream('php://qweqweqw', 0);

        $loader->load(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'id_1')),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'id_2')),
                Row::create(Entry::integer('id', 3), Entry::string('name', 'id_3'))
            )
        );
    }

    public function test_loading_rows_and_schema_into_php_memory_stream() : void
    {
        $loader = To::output(false, StreamLoader\Output::rows_and_schema);

        \ob_start();

        $loader->load(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'id_1')),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'id_2')),
                Row::create(Entry::integer('id', 3), Entry::string('name', 'id_3'))
            )
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        $this->assertSame(
            <<<'ASCII'
+--+----+
|id|name|
+--+----+
| 1|id_1|
| 2|id_2|
| 3|id_3|
+--+----+
3 rows

schema
|-- id: Flow\ETL\Row\Entry\IntegerEntry (nullable = false)
|-- name: Flow\ETL\Row\Entry\StringEntry (nullable = false)

ASCII,
            $output
        );
    }

    public function test_loading_rows_into_php_memory_stream() : void
    {
        $loader = new StreamLoader('php://output', Mode::WRITE, 0);

        \ob_start();

        $loader->load(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'id_1')),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'id_2')),
                Row::create(Entry::integer('id', 3), Entry::string('name', 'id_3'))
            )
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        $this->assertStringContainsString(
            <<<'TABLE'
+--+----+
|id|name|
+--+----+
| 1|id_1|
| 2|id_2|
| 3|id_3|
+--+----+
3 rows
TABLE,
            $output
        );
    }

    public function test_loading_schema_into_php_memory_stream() : void
    {
        $loader = new StreamLoader('php://output', Mode::WRITE, 0, StreamLoader\Output::schema);

        \ob_start();

        $loader->load(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'id_1')),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'id_2')),
                Row::create(Entry::integer('id', 3), Entry::string('name', 'id_3'))
            )
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        $this->assertSame(
            <<<'ASCII'
schema
|-- id: Flow\ETL\Row\Entry\IntegerEntry (nullable = false)
|-- name: Flow\ETL\Row\Entry\StringEntry (nullable = false)

ASCII,
            $output
        );
    }
}
