<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

interface ContextStorage
{
    public function attach(Context $context): Scope;

    public function current(): Context;
}
