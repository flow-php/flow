<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\{config, flow_context};
use function Flow\ETL\DSL\{int_entry, ref, row, rows, str_entry, to_output, to_stream};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\{Tests\FlowTestCase};
use Flow\Filesystem\Stream\Mode;

final class StreamLoaderTest extends FlowTestCase
{
    public function test_columns_count_to_php_output_stream() : void
    {
        $loader = to_output(false, Output::column_count);

        \ob_start();

        $loader->load(
            rows(
                row(int_entry('id', 1), str_entry('name', 'id_1')),
                row(int_entry('id', 2), str_entry('name', 'id_2')),
                row(int_entry('id', 3), str_entry('name', 'id_3'))
            ),
            flow_context(config())
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        self::assertSame(
            <<<'ASCII'
Columns: 2

ASCII,
            $output
        );
    }

    public function test_loading_data_into_invalid_stream() : void
    {
        $this->expectExceptionMessage("Can't open stream for url: php://qweqweqw in mode: w");
        $this->expectException(RuntimeException::class);

        $loader = to_stream('php://qweqweqw', 0);

        $loader->load(
            rows(
                row(int_entry('id', 1), str_entry('name', 'id_1')),
                row(int_entry('id', 2), str_entry('name', 'id_2')),
                row(int_entry('id', 3), str_entry('name', 'id_3'))
            ),
            flow_context(config())
        );
    }

    public function test_loading_partitioned_rows_into_php_output_stream() : void
    {
        $loader = new StreamLoader('php://output', Mode::WRITE, 0);

        \ob_start();

        $loader->load(
            rows(
                row(int_entry('id', 1), str_entry('name', 'id_1'), str_entry('group', 'a')),
                row(int_entry('id', 2), str_entry('name', 'id_2'), str_entry('group', 'a')),
                row(int_entry('id', 3), str_entry('name', 'id_3'), str_entry('group', 'a'))
            )->partitionBy(ref('group'))[0],
            flow_context(config())
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        self::assertStringContainsString(
            <<<'TABLE'
+----+------+-------+
| id | name | group |
+----+------+-------+
|  1 | id_1 |     a |
|  2 | id_2 |     a |
|  3 | id_3 |     a |
+----+------+-------+
Partitions:
 - group=a
3 rows
TABLE,
            $output
        );
    }

    public function test_loading_rows_and_schema_into_output_stream() : void
    {
        $loader = to_output(false, Output::rows_and_schema);

        \ob_start();

        $loader->load(
            rows(
                row(int_entry('id', 1), str_entry('name', 'id_1')),
                row(int_entry('id', 2), str_entry('name', 'id_2')),
                row(int_entry('id', 3), str_entry('name', 'id_3'))
            ),
            flow_context(config())
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        self::assertSame(
            <<<'ASCII'
+----+------+
| id | name |
+----+------+
|  1 | id_1 |
|  2 | id_2 |
|  3 | id_3 |
+----+------+
3 rows

schema
|-- id: integer
|-- name: string

ASCII,
            $output
        );
    }

    public function test_loading_rows_into_php_output_stream() : void
    {
        $loader = new StreamLoader('php://output', Mode::WRITE, 0);

        \ob_start();

        $loader->load(
            rows(
                row(int_entry('id', 1), str_entry('name', 'id_1')),
                row(int_entry('id', 2), str_entry('name', 'id_2')),
                row(int_entry('id', 3), str_entry('name', 'id_3'))
            ),
            flow_context(config())
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        self::assertStringContainsString(
            <<<'TABLE'
+----+------+
| id | name |
+----+------+
|  1 | id_1 |
|  2 | id_2 |
|  3 | id_3 |
+----+------+
3 rows
TABLE,
            $output
        );
    }

    public function test_loading_schema_into_php_output_stream() : void
    {
        $loader = new StreamLoader('php://output', Mode::WRITE, 0, Output::schema);

        \ob_start();

        $loader->load(
            rows(
                row(int_entry('id', 1), str_entry('name', 'id_1')),
                row(int_entry('id', 2), str_entry('name', 'id_2')),
                row(int_entry('id', 3), str_entry('name', 'id_3'))
            ),
            flow_context(config())
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        self::assertSame(
            <<<'ASCII'
schema
|-- id: integer
|-- name: string

ASCII,
            $output
        );
    }

    public function test_rows_and_columns_count_to_php_output_stream() : void
    {
        $loader = to_output(false, Output::rows_and_column_count);

        \ob_start();

        $loader->load(
            rows(
                row(int_entry('id', 1), str_entry('name', 'id_1')),
                row(int_entry('id', 2), str_entry('name', 'id_2')),
                row(int_entry('id', 3), str_entry('name', 'id_3'))
            ),
            flow_context(config())
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        self::assertSame(
            <<<'ASCII'
Rows: 3, Columns: 2

ASCII,
            $output
        );
    }

    public function test_rows_count_to_php_output_stream() : void
    {
        $loader = to_output(false, Output::rows_count);

        \ob_start();

        $loader->load(
            rows(
                row(int_entry('id', 1), str_entry('name', 'id_1')),
                row(int_entry('id', 2), str_entry('name', 'id_2')),
                row(int_entry('id', 3), str_entry('name', 'id_3'))
            ),
            flow_context(config())
        );
        $output = \ob_get_contents();
        \ob_end_clean();

        self::assertSame(
            <<<'ASCII'
Rows: 3

ASCII,
            $output
        );
    }
}
