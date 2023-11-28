<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use function Flow\ETL\DSL\from_array;
use Flow\ETL\DSL\CSV;
use Flow\ETL\Flow;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\ETL\Transformer\LimitTransformer;

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
            ->write(CSV::to($path))
            ->run();

        $this->assertSame(
            3,
            (new Flow())
                ->read(CSV::from($path))
                ->transform(new LimitTransformer(3))
                ->count()
        );
    }
}
