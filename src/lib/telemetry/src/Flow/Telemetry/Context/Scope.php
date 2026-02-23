<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

interface Scope
{
    public function detach() : int;
}
