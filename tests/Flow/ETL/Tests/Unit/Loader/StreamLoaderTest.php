<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class StreamLoaderTest extends TestCase
{
    public function test_loading_data_into_php_memory_stream() : void
    {
        $loader = new StreamLoader('php://output', 'w', 0);

        \ob_start();

        $loader->load(
            new Rows(
                Row::create(new IntegerEntry('id', 1), new StringEntry('name', 'id_1')),
                Row::create(new IntegerEntry('id', 2), new StringEntry('name', 'id_2')),
                Row::create(new IntegerEntry('id', 3), new StringEntry('name', 'id_3'))
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

    public function test_loading_data_int_invalid_stream() : void
    {
        $this->expectExceptionMessage("Can't open stream for url: php://qweqweqw in mode: w. Reason: fopen(): Invalid php:// URL specified");
        $this->expectException(RuntimeException::class);

        $loader = new StreamLoader('php://qweqweqw', 'w', 0);

        $loader->load(
            new Rows(
                Row::create(new IntegerEntry('id', 1), new StringEntry('name', 'id_1')),
                Row::create(new IntegerEntry('id', 2), new StringEntry('name', 'id_2')),
                Row::create(new IntegerEntry('id', 3), new StringEntry('name', 'id_3'))
            )
        );
    }
}
