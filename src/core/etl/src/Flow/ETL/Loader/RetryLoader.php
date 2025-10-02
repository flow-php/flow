<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\{Exception\FailedRetryException, FlowContext, Loader, Rows};
use Flow\ETL\Retry\DelayFactory\Fixed\FixedMilliseconds;
use Flow\ETL\Retry\{DelayFactory, FailedRetry, RetriesRecord, RetryStrategy};
use Flow\ETL\Retry\RetryStrategy\AnyThrowable;
use Flow\ETL\Time\{Sleep, SystemSleep};

final readonly class RetryLoader implements Loader
{
    public function __construct(
        private Loader $loader,
        private RetryStrategy $retryStrategy = new AnyThrowable(3),
        private DelayFactory $delayFactory = new FixedMilliseconds(200),
        private Sleep $sleep = new SystemSleep(),
    ) {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $attemptNumber = 0;
        $retriesRecord = new RetriesRecord();

        while (true) {
            $attemptNumber++;

            try {
                $this->loader->load($rows, $context);

                return;
            } catch (\Throwable $exception) {
                $retriesRecord->add(FailedRetry::create($context->config->clock(), $exception, $attemptNumber));

                if (!$this->retryStrategy->shouldRetry($exception, $attemptNumber)) {
                    throw new FailedRetryException($retriesRecord);
                }

                $this->sleep->for($this->delayFactory->delay($attemptNumber));
            }
        }
    }
}
