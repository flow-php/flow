<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\from_array;
use Flow\ETL\Flow;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class SynchronousPipelineTest extends IntegrationTestCase
{
    public function test_limit() : void
    {
        $path = \sys_get_temp_dir() . '/synchronous_pipeline_' . __FUNCTION__ . '.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ]))
            ->write(to_csv($path))
            ->run();

        self::assertSame(
            3,
            (new Flow())
                ->read(from_csv($path))
                ->limit(3)
                ->count()
        );
    }
}
