<?php

declare(strict_types=1);

namespace Flow\ETL\Retry;

interface RetryStrategy
{
    public function shouldRetry(\Throwable $exception, int $attemptNumber) : bool;
}
