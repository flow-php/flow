<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function file_exists;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\ref;
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
