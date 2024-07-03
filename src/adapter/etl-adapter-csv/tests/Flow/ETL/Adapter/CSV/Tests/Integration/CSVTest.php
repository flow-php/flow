<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{array_entry, df, int_entry, overwrite, ref, row, rows};
use Flow\ETL\Flow;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class CSVTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_loading_array_entry() : void
    {
        $this->expectExceptionMessage('Entry "data" is an list|array, please cast to string before writing to CSV. Easiest way to cast arrays to string is to use Transform::to_json transformer.');

        (new Flow())
            ->process(rows(row(int_entry('id', 1), array_entry('data', ['foo' => 'bar']))))
            ->write(to_csv(__DIR__ . '/var/test_loading_array_entry.csv'))
            ->run();
    }

    public function test_loading_csv_files() : void
    {

        df()
            ->read(new FakeExtractor(100))
            ->drop('array', 'list', 'map', 'struct', 'object', 'enum', 'list_of_datetimes')
            ->withEntry('datetime', ref('datetime')->dateFormat('Y-m-d H:i:s'))
            ->saveMode(overwrite())
            ->load(to_csv($path = __DIR__ . '/var/test_loading_csv_files.csv'))
            ->run();

        self::assertEquals(
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
