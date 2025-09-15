<?php

declare(strict_types=1);

namespace Flow\ETL\Retry;

use Flow\ETL\Time\Duration;

interface DelayFactory
{
    public function delay(int $attempt) : Duration;
}
