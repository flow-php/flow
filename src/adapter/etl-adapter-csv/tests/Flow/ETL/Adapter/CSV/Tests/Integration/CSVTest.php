<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\Tests\Double\FakeExtractor;
use PHPUnit\Framework\TestCase;

final class CSVTest extends TestCase
{
    public function test_loading_array_entry() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        $this->expectExceptionMessage('Entry "data" is an list|array, please cast to string before writing to CSV. Easiest way to cast arrays to string is to use Transform::to_json transformer.');

        (new Flow())
            ->process(rows(row(int_entry('id', 1), array_entry('data', ['foo' => 'bar']))))
            ->write(to_csv($path))
            ->run();
    }

    public function test_loading_csv_files() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(new FakeExtractor(100))
            ->drop('array', 'list', 'map', 'struct', 'object', 'enum', 'list_of_datetimes')
            ->withEntry('datetime', ref('datetime')->dateFormat('Y-m-d H:i:s'))
            ->load(to_csv($path))
            ->run();

        $this->assertEquals(
            100,
            df()->read(from_csv($path))->count()
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("CSVLoader path can't be pattern, given: /path/*/pattern.csv");

        to_csv(new Path('/path/*/pattern.csv'));
    }
}
