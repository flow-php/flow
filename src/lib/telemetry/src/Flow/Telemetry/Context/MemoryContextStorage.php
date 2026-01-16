<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

final class MemoryContextStorage implements ContextStorage
{
    private Context $context;

    public function __construct(?Context $context = null)
    {
        $this->context = $context ?? Context::create();
    }

    public function current() : Context
    {
        return $this->context;
    }

    public function store(Context $context) : void
    {
        $this->context = $context;
    }
}
