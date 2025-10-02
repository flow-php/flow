<?php

declare(strict_types=1);

namespace Flow\ETL\Retry\DelayFactory\Fixed;

use Flow\ETL\Retry\DelayFactory;
use Flow\ETL\Time\Duration;

final readonly class FixedMilliseconds implements DelayFactory
{
    public function __construct(private int $milliseconds)
    {

    }

    public function delay(int $attempt) : Duration
    {
        return Duration::fromMilliseconds($this->milliseconds);
    }
}
