<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

interface Scope
{
    public const int DETACHED = 0;

    public const int INACTIVE = 1;

    public const int MISMATCH = 2;

    public function detach(): int;
}
