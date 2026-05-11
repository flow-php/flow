<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

final class ContextScope implements Scope
{
    public const int DETACHED = 0;

    private bool $detached = false;

    public function __construct(
        private readonly Context $previous,
        private readonly MemoryContextStorage $storage,
    ) {}

    public function detach(): int
    {
        if ($this->detached) {
            return self::DETACHED;
        }

        $this->detached = true;
        $this->storage->store($this->previous);

        return self::DETACHED;
    }
}
