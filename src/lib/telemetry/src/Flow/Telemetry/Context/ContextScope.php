<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

final class ContextScope implements Scope
{
    private bool $detached = false;

    public function __construct(
        private readonly int $depth,
        private readonly MemoryContextStorage $storage,
    ) {}

    public function detach(): int
    {
        if ($this->detached) {
            return self::INACTIVE;
        }

        $this->detached = true;

        return $this->storage->detachAt($this->depth);
    }
}
