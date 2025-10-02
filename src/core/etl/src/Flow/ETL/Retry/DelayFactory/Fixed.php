<?php

declare(strict_types=1);

namespace Flow\ETL\Retry\DelayFactory;

use Flow\ETL\Retry\DelayFactory;
use Flow\ETL\Time\Duration;

final readonly class Fixed implements DelayFactory
{
    public function __construct(private Duration $duration)
    {
    }

    public function delay(int $attempt) : Duration
    {
        return $this->duration;
    }
}
