<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Runtime;

use Flow\Bridge\Symfony\TelemetryBundle\Runtime\EnvironmentWorkerModeDetector;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Context\RuntimeEnvironment;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(EnvironmentWorkerModeDetector::class)]
final class EnvironmentWorkerModeDetectorTest extends TestCase
{
    public function test_detects_worker_from_app_runtime_mode(): void
    {
        RuntimeEnvironment::with(
            ['APP_RUNTIME_MODE' => 'web=1&worker=1', 'FRANKENPHP_WORKER' => null, 'RR_MODE' => null],
            ['RR_MODE' => null],
            function (): void {
                static::assertTrue((new EnvironmentWorkerModeDetector())->detect());
            },
        );
    }

    public function test_detects_worker_from_frankenphp_worker(): void
    {
        RuntimeEnvironment::with(
            ['APP_RUNTIME_MODE' => null, 'FRANKENPHP_WORKER' => '1', 'RR_MODE' => null],
            ['RR_MODE' => null],
            function (): void {
                static::assertTrue((new EnvironmentWorkerModeDetector())->detect());
            },
        );
    }

    public function test_detects_worker_from_roadrunner_env_fallback(): void
    {
        RuntimeEnvironment::with(
            ['APP_RUNTIME_MODE' => null, 'FRANKENPHP_WORKER' => null, 'RR_MODE' => null],
            ['RR_MODE' => 'http'],
            function (): void {
                static::assertTrue((new EnvironmentWorkerModeDetector())->detect());
            },
        );
    }

    public function test_detects_worker_from_roadrunner_server_mode(): void
    {
        RuntimeEnvironment::with(
            ['APP_RUNTIME_MODE' => null, 'FRANKENPHP_WORKER' => null, 'RR_MODE' => 'http'],
            ['RR_MODE' => null],
            function (): void {
                static::assertTrue((new EnvironmentWorkerModeDetector())->detect());
            },
        );
    }

    public function test_does_not_detect_worker_from_disabled_frankenphp_worker(): void
    {
        RuntimeEnvironment::with(
            ['APP_RUNTIME_MODE' => null, 'FRANKENPHP_WORKER' => '0', 'RR_MODE' => null],
            ['RR_MODE' => null],
            function (): void {
                static::assertFalse((new EnvironmentWorkerModeDetector())->detect());
            },
        );
    }

    public function test_does_not_detect_worker_from_web_only_app_runtime_mode(): void
    {
        RuntimeEnvironment::with(
            ['APP_RUNTIME_MODE' => 'web=1', 'FRANKENPHP_WORKER' => null, 'RR_MODE' => null],
            ['RR_MODE' => null],
            function (): void {
                static::assertFalse((new EnvironmentWorkerModeDetector())->detect());
            },
        );
    }

    public function test_does_not_detect_worker_without_any_signal(): void
    {
        RuntimeEnvironment::with(
            ['APP_RUNTIME_MODE' => null, 'FRANKENPHP_WORKER' => null, 'RR_MODE' => null],
            ['RR_MODE' => null],
            function (): void {
                static::assertFalse((new EnvironmentWorkerModeDetector())->detect());
            },
        );
    }
}
