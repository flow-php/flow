<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Runtime;

use Flow\Bridge\Symfony\TelemetryBundle\Runtime\RuntimeModeResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Runtime\StubWorkerModeDetector;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ValueError;

#[CoversClass(RuntimeModeResolver::class)]
final class RuntimeModeResolverTest extends TestCase
{
    public function test_auto_mode_is_not_worker_when_detector_reports_classic(): void
    {
        $resolver = new RuntimeModeResolver('auto', new StubWorkerModeDetector(false));

        static::assertFalse($resolver->isWorker());
    }

    public function test_auto_mode_is_worker_when_detector_reports_worker(): void
    {
        $resolver = new RuntimeModeResolver('auto', new StubWorkerModeDetector(true));

        static::assertTrue($resolver->isWorker());
    }

    public function test_classic_mode_is_never_worker_even_when_detector_reports_worker(): void
    {
        $resolver = new RuntimeModeResolver('classic', new StubWorkerModeDetector(true));

        static::assertFalse($resolver->isWorker());
    }

    public function test_invalid_mode_is_rejected(): void
    {
        $this->expectException(ValueError::class);

        new RuntimeModeResolver('invalid', new StubWorkerModeDetector(false));
    }

    public function test_worker_mode_is_always_worker_even_when_detector_reports_classic(): void
    {
        $resolver = new RuntimeModeResolver('worker', new StubWorkerModeDetector(false));

        static::assertTrue($resolver->isWorker());
    }
}
