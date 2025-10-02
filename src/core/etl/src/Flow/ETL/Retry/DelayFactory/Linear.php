<?php

declare(strict_types=1);

namespace Flow\ETL\Retry\DelayFactory;

use Flow\ETL\Retry\DelayFactory;
use Flow\ETL\Time\Duration;

final readonly class Linear implements DelayFactory
{
    public function __construct(
        private Duration $baseDuration,
        private Duration $increment,
    ) {
    }

    public function delay(int $attempt) : Duration
    {
        return Duration::fromMicroseconds(
            $this->baseDuration->microseconds() + ($this->increment->microseconds() * ($attempt - 1))
        );
    }
}
