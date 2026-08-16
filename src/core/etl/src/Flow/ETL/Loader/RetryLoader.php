<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\FailedRetryException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Retry\DelayFactory;
use Flow\ETL\Retry\DelayFactory\Fixed\FixedMilliseconds;
use Flow\ETL\Retry\FailedRetry;
use Flow\ETL\Retry\RetriesRecord;
use Flow\ETL\Retry\RetryStrategy;
use Flow\ETL\Retry\RetryStrategy\AnyThrowableExcept;
use Flow\ETL\Rows;
use Flow\ETL\Time\Sleep;
use Flow\ETL\Time\SystemSleep;
use Throwable;

final readonly class RetryLoader implements Closure, Loader, OverridingLoader
{
    public function __construct(
        private Loader $loader,
        private RetryStrategy $retryStrategy = new AnyThrowableExcept([InvalidLogicException::class], 3),
        private DelayFactory $delayFactory = new FixedMilliseconds(200),
        private Sleep $sleep = new SystemSleep(),
    ) {
        // Retries are per batch, but a Transformation drives one nested pipeline across the whole stream: by the time
        // a batch fails, the pipeline has already consumed the batches after it, so re-offering that batch feeds it
        // into a pipeline that moved on. Retrying the destination instead is well defined.
        if ($this->loader instanceof TransformerLoader) {
            throw new InvalidLogicException(
                'RetryLoader cannot wrap a TransformerLoader, retries are per batch while a Transformation spans the '
                . 'whole stream. Retry the destination instead: '
                . 'to_transformation($transformation, write_with_retries($loader)).',
            );
        }
    }

    public function closure(FlowContext $context): void
    {
        if ($this->loader instanceof Closure) {
            $this->loader->closure($context);
        }
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            $attemptNumber = 0;
            $retriesRecord = new RetriesRecord();

            while (true) {
                $attemptNumber++;

                try {
                    $this->loader->load($rows, $context);

                    $context->telemetry()->loadingCompleted($this, [
                        TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count(),
                    ]);

                    return;
                } catch (Throwable $exception) {
                    $retriesRecord->add(FailedRetry::create($context->config->clock(), $exception, $attemptNumber));

                    if (!$this->retryStrategy->shouldRetry($exception, $attemptNumber)) {
                        throw new FailedRetryException($retriesRecord);
                    }

                    $this->sleep->for($this->delayFactory->delay($attemptNumber));
                }
            }
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function loaders(): array
    {
        return [
            $this->loader,
        ];
    }
}
