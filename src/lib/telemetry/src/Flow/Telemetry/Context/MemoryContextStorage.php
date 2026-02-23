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

    public function attach(Context $context) : Scope
    {
        $previous = $this->context;
        $this->context = $context;

        return new ContextScope($previous, $this);
    }

    public function current() : Context
    {
        return $this->context;
    }

    /**
     * Internal method for scope detachment.
     *
     * @internal
     */
    public function store(Context $context) : void
    {
        $this->context = $context;
    }
}
