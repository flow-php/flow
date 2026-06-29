<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Runtime;

interface WorkerModeDetector
{
    public function detect(): bool;
}
