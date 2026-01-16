<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

interface ContextStorage
{
    public function current() : Context;

    public function store(Context $context) : void;
}
