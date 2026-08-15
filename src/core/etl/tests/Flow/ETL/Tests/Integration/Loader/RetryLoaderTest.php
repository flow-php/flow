<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Loader;

use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\write_with_retries;

final class RetryLoaderTest extends FlowIntegrationTestCase
{
    public function test_retry_loader_closes_the_wrapped_loader_once_per_run(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]))
            ->batchSize(2)
            ->write(write_with_retries($spy))
            ->run();

        static::assertSame(2, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }
}
