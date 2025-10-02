<?php

declare(strict_types=1);

namespace Flow\ETL\Retry\DelayFactory;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Retry\DelayFactory;
use Flow\ETL\Time\Duration;

final readonly class Jitter implements DelayFactory
{
    public function __construct(
        private DelayFactory $delayFactory,
        private float $jitterPercentage,
    ) {
        if ($jitterPercentage < 0.0 || $jitterPercentage > 1.0) {
            throw new InvalidArgumentException('Jitter percentage must be between 0.0 and 1.0');
        }
    }

    public function delay(int $attempt) : Duration
    {
        $baseDelay = $this->delayFactory->delay($attempt);

        if ($this->jitterPercentage === 0.0) {
            return $baseDelay;
        }

        $jitterRange = (int) ($baseDelay->microseconds() * $this->jitterPercentage);
        $jitter = \mt_rand(-$jitterRange, $jitterRange);

        $jitteredDelay = $baseDelay->microseconds() + $jitter;

        return Duration::fromMicroseconds(\max(0, $jitteredDelay));
    }
}
