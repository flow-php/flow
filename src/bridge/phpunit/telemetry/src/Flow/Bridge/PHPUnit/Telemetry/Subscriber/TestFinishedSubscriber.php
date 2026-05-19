<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\TestStatusRegistry;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanStatus;
use PHPUnit\Event\Test\Finished;
use PHPUnit\Event\Test\FinishedSubscriber;
use Throwable;

final readonly class TestFinishedSubscriber implements FinishedSubscriber
{
    public function __construct(
        private Telemetry $telemetry,
        private SpanStack $spanStack,
        private Configuration $config,
        private TestStatusRegistry $statusRegistry,
    ) {}

    public function notify(Finished $event): void
    {
        try {
            $testId = $event->test()->id();
            $status = $this->statusRegistry->getStatus($testId);

            if (!$this->config->emitTestSpans) {
                if ($this->config->emitMetrics) {
                    $meter = $this->telemetry->meter('phpunit', PackageVersion::get('phpunit/phpunit'));

                    $meter->createCounter('phpunit.test.count')->add(1, ['test.status' => $status]);
                }

                $this->statusRegistry->clear($testId);

                return;
            }

            $span = $this->spanStack->pop();

            if ($span === null) {
                return;
            }

            $errorMessage = $this->statusRegistry->getMessage($testId);
            $tracer = $this->telemetry->tracer('phpunit', PackageVersion::get('phpunit/phpunit'));

            $span->setAttribute('test.status', $status);
            $span->end();

            $duration = $span->duration();

            if ($duration !== null) {
                $span->setAttribute('test.duration_ms', $duration);
            }

            if ($errorMessage !== null) {
                $span->setAttribute('exception.message', $errorMessage);
            }

            if ($status === 'passed') {
                $span->setStatus(SpanStatus::ok());
            } else {
                $span->setStatus(SpanStatus::error($errorMessage ?? $status));
            }

            if ($this->config->emitMetrics && $duration !== null) {
                $meter = $this->telemetry->meter('phpunit', PackageVersion::get('phpunit/phpunit'));

                $meter->createHistogram('phpunit.test.duration', 'ms')->record($duration, ['test.status' => $status]);

                $meter->createCounter('phpunit.test.count')->add(1, ['test.status' => $status]);
            }

            $tracer->complete($span);
            $this->statusRegistry->clear($testId);
        } catch (Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
