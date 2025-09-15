<?php

declare(strict_types=1);

namespace Flow\ETL\Retry\RetryStrategy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Retry\RetryStrategy;

final readonly class AnyThrowable implements RetryStrategy
{
    public function __construct(private int $limit)
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException('Retry limit must be greater than 0');
        }
    }

    public function shouldRetry(\Throwable $exception, int $attemptNumber) : bool
    {
        return $attemptNumber <= $this->limit;
    }
}
