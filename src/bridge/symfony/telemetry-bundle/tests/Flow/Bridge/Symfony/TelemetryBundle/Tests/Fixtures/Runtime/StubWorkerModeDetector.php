<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Runtime;

use Flow\Bridge\Symfony\TelemetryBundle\Runtime\WorkerModeDetector;

final class StubWorkerModeDetector implements WorkerModeDetector
{
    public function __construct(
        private readonly bool $isWorker,
    ) {}

    public function detect(): bool
    {
        return $this->isWorker;
    }
}
