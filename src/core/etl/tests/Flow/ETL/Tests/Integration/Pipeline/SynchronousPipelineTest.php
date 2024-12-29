<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{df, from_array, lit};
use Flow\ETL\Loader;
use Flow\ETL\Tests\Integration\FlowIntegrationTestCase;

final class SynchronousPipelineTest extends FlowIntegrationTestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_limit() : void
    {
        $path = __DIR__ . '/var/synchronous_pipeline_' . __FUNCTION__ . '.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
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
            df()
                ->read(from_csv($path))
                ->limit(3)
                ->count()
        );
    }

    public function test_not_calling_loader_when_rows_are_empty() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::never())->method('load');

        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ]))
            ->filter(lit(true)->equals(false))
            ->write($loader)
            ->run();
    }
}
