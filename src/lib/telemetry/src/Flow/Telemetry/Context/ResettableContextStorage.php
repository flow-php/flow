<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

interface ResettableContextStorage
{
    public function reset(): void;
}
