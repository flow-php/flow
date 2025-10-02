<?php

declare(strict_types=1);

namespace Flow\ETL\Retry\DelayFactory;

use Flow\ETL\Retry\DelayFactory;
use Flow\ETL\Time\Duration;

final readonly class Exponential implements DelayFactory
{
    public function __construct(
        private Duration $baseDuration,
        private int $multiplier = 2,
        private ?Duration $maxDelay = null,
    ) {
    }

    public function delay(int $attempt) : Duration
    {
        $calculatedDelay = Duration::fromMicroseconds(
            (int) ($this->baseDuration->microseconds() * ($this->multiplier ** ($attempt - 1)))
        );

        if ($this->maxDelay !== null && $calculatedDelay->microseconds() > $this->maxDelay->microseconds()) {
            return $this->maxDelay;
        }

        return $calculatedDelay;
    }
}
