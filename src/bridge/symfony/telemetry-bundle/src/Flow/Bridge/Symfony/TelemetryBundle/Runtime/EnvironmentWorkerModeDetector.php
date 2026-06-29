<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Runtime;

use function getenv;
use function is_string;
use function parse_str;

final class EnvironmentWorkerModeDetector implements WorkerModeDetector
{
    public function detect(): bool
    {
        $runtimeMode = $_SERVER['APP_RUNTIME_MODE'] ?? null;

        if (is_string($runtimeMode) && $runtimeMode !== '') {
            $parsed = [];
            parse_str($runtimeMode, $parsed);

            if (($parsed['worker'] ?? null) === '1') {
                return true;
            }
        }

        if ($this->isTruthy($_SERVER['FRANKENPHP_WORKER'] ?? null)) {
            return true;
        }

        $roadRunnerMode = $_SERVER['RR_MODE'] ?? getenv('RR_MODE');

        return is_string($roadRunnerMode) && $roadRunnerMode !== '';
    }

    private function isTruthy(mixed $value): bool
    {
        if (is_string($value)) {
            return $value !== '' && $value !== '0' && $value !== 'false';
        }

        return $value === true || $value === 1;
    }
}
