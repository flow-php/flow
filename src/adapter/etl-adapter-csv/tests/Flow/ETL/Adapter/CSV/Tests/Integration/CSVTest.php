<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function file_exists;
use function file_get_contents;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\ref;
use function implode;
use function mkdir;
use function unlink;

final class CSVTest extends FlowTestCase
{
    protected function setUp(): void
    {
        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    public function test_loading_csv_rows_of_decreasing_length_across_multiple_batches(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'value' => 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
                ['id' => 2, 'value' => 'b'],
                ['id' => 3, 'value' => 'cc'],
                ['id' => 4, 'value' => 'd'],
            ]))
            ->batchSize(2)
            ->saveMode(overwrite())
            ->load(to_csv($path = __DIR__ . '/var/test_loading_csv_rows_of_decreasing_length.csv'))
            ->run();

        static::assertSame(
            implode(PHP_EOL, ['id,value', '1,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '2,b', '3,cc', '4,d', '']),
            file_get_contents($path),
        );

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_loading_csv_files(): void
    {
        df()
            ->read(new FakeExtractor(100))
            ->drop('array', 'list', 'map', 'struct', 'object', 'enum', 'list_of_datetimes')
            ->withEntry('datetime', ref('datetime')->dateFormat('Y-m-d H:i:s'))
            ->saveMode(overwrite())
            ->load(to_csv($path = __DIR__ . '/var/test_loading_csv_files.csv'))
            ->run();

        static::assertEquals(100, df()->read(from_csv($path))->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }
}
