<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Loader;

use Flow\ETL\Exception\FailedRetryException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingLoader;
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

    public function test_retry_loader_does_not_retry_an_invalid_logic_exception(): void
    {
        // The default strategy declines InvalidLogicException: a pipeline definition error can never succeed on a
        // retry, so the attempt is made once instead of four times and no delay is slept. RetryLoader still wraps a
        // declined exception in FailedRetryException, so the actionable message arrives as the previous exception
        // rather than the top-level one.
        $loader = new ThrowingLoader(new InvalidLogicException('pipeline definition error'));

        try {
            df()
                ->read(from_array([['id' => 2], ['id' => 1]]))
                ->write(write_with_retries($loader))
                ->run();

            static::fail('Expected the InvalidLogicException to be declined by the retry strategy.');
        } catch (FailedRetryException $e) {
            static::assertSame(1, $e->record->count());
            static::assertInstanceOf(InvalidLogicException::class, $e->getPrevious());
            static::assertSame(1, $loader->loadsCount);
        }
    }
}
