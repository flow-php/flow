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
    private LoaderTree $loaderTree;

    public function __construct(
        private Loader $loader,
        private RetryStrategy $retryStrategy = new AnyThrowableExcept([InvalidLogicException::class], 3),
        private DelayFactory $delayFactory = new FixedMilliseconds(200),
        private Sleep $sleep = new SystemSleep(),
    ) {
        $this->loaderTree = new LoaderTree();
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
            // Retries re-offer the failed batch to the same loader instance. A loader that holds state across load()
            // calls which cannot be rewound may have already advanced past this batch - and Flow cannot determine
            // from outside whether re-offering is safe (statefulness of a wrapped Transformer is undetectable; a
            // Transformation's drive has consumed later batches). The whole wrapped tree is checked, because a
            // wrapper that is itself replay-safe can hold one that is not. Checked per load() because
            // withTransformation() can arm a drive after this wrapper was built; placed before the retry loop so the
            // refusal surfaces as itself and is never re-reported as a FailedRetryException after N attempts.
            foreach ($this->loaderTree->flatten($this->loader) as $wrapped) {
                if ($wrapped instanceof ReplayAware && !$wrapped->replaySafe()) {
                    throw new InvalidLogicException(
                        'RetryLoader cannot wrap this loader: it holds state across load() calls that cannot be '
                        . 'rewound, so Flow cannot tell whether re-offering a failed batch is safe. Retry the '
                        . 'destination instead: to_transformation($transformation, write_with_retries($loader)) or '
                        . 'to_branch($condition, write_with_retries($loader))->withTransformation($transformation).',
                    );
                }
            }

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
