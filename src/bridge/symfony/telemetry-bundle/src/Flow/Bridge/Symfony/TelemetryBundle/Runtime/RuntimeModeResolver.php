<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Runtime;

final readonly class RuntimeModeResolver
{
    private RuntimeMode $mode;

    public function __construct(
        string $mode,
        private WorkerModeDetector $detector,
    ) {
        $this->mode = RuntimeMode::from($mode);
    }

    public function isWorker(): bool
    {
        return match ($this->mode) {
            RuntimeMode::Worker => true,
            RuntimeMode::Classic => false,
            RuntimeMode::Auto => $this->detector->detect(),
        };
    }
}
