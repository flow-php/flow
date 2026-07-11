<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\PHPUnitTelemetryAttributes;
use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\SuiteOutcomeStack;
use Flow\Bridge\PHPUnit\Telemetry\TestMemoryRegistry;
use Flow\Bridge\PHPUnit\Telemetry\TestStatusRegistry;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanStatus;
use PHPUnit\Event\Test\Finished;
use PHPUnit\Event\Test\FinishedSubscriber;
use Throwable;

use function memory_get_peak_usage;
use function memory_get_usage;

final readonly class TestFinishedSubscriber implements FinishedSubscriber
{
    /**
     * OTel semconv-advised bucket boundaries for short operation durations in seconds.
     */
    private const array DURATION_BOUNDARIES = [
        0.005,
        0.01,
        0.025,
        0.05,
        0.075,
        0.1,
        0.25,
        0.5,
        0.75,
        1.0,
        2.5,
        5.0,
        7.5,
        10.0,
    ];

    public function __construct(
        private Telemetry $telemetry,
        private SpanStack $spanStack,
        private Configuration $config,
        private TestStatusRegistry $statusRegistry,
        private TestMemoryRegistry $memoryRegistry,
        private SuiteOutcomeStack $suiteOutcomes,
    ) {}

    public function notify(Finished $event): void
    {
        try {
            $testId = $event->test()->id();
            $status = $this->statusRegistry->getStatus($testId);
            $this->suiteOutcomes->recordStatus($status);

            $startBytes = $this->memoryRegistry->getStart($testId);
            $peakBytes = memory_get_peak_usage($this->config->memoryRealUsage);
            $deltaBytes = $startBytes !== null ? memory_get_usage($this->config->memoryRealUsage) - $startBytes : null;
            $this->memoryRegistry->clear($testId);

            if (!$this->config->emitTestSpans) {
                if ($this->config->emitMetrics) {
                    $meter = $this->telemetry->meter('phpunit', PackageVersion::get('phpunit/phpunit'));

                    $meter->createCounter('flow.phpunit.test.count', '{test}')->add(1, [
                        SemConvAttributes::TEST_CASE_RESULT_STATUS => $status,
                    ]);
                    $meter->createHistogram('flow.phpunit.test.memory.peak', 'By')->record($peakBytes, [
                        SemConvAttributes::TEST_CASE_RESULT_STATUS => $status,
                    ]);

                    if ($deltaBytes !== null) {
                        $meter->createHistogram('flow.phpunit.test.memory.delta', 'By')->record($deltaBytes, [
                            SemConvAttributes::TEST_CASE_RESULT_STATUS => $status,
                        ]);
                    }
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

            $span->setAttribute(SemConvAttributes::TEST_CASE_RESULT_STATUS, $status);
            $span->end();

            $duration = $span->duration();

            $span->setAttribute(PHPUnitTelemetryAttributes::ATTR_TEST_MEMORY_PEAK, $peakBytes);

            if ($deltaBytes !== null) {
                $span->setAttribute(PHPUnitTelemetryAttributes::ATTR_TEST_MEMORY_DELTA, $deltaBytes);
            }

            // OTEL spec: instrumentation leaves the status Unset on success; only failures set a status.
            if ($status !== 'passed') {
                $span->setAttribute(SemConvAttributes::ERROR_TYPE, $status);
                $span->setStatus(SpanStatus::error($errorMessage ?? $status));
            }

            if ($this->config->emitMetrics) {
                $meter = $this->telemetry->meter('phpunit', PackageVersion::get('phpunit/phpunit'));

                if ($duration !== null) {
                    $meter->createHistogram(
                        'flow.phpunit.test.duration',
                        's',
                        'Duration of a single test',
                        self::DURATION_BOUNDARIES,
                    )->record($duration / 1_000, [
                        SemConvAttributes::TEST_CASE_RESULT_STATUS => $status,
                    ]);

                    $meter->createCounter('flow.phpunit.test.count', '{test}')->add(1, [
                        SemConvAttributes::TEST_CASE_RESULT_STATUS => $status,
                    ]);
                }

                $meter->createHistogram('flow.phpunit.test.memory.peak', 'By')->record($peakBytes, [
                    SemConvAttributes::TEST_CASE_RESULT_STATUS => $status,
                ]);

                if ($deltaBytes !== null) {
                    $meter->createHistogram('flow.phpunit.test.memory.delta', 'By')->record($deltaBytes, [
                        SemConvAttributes::TEST_CASE_RESULT_STATUS => $status,
                    ]);
                }
            }

            $tracer->complete($span);
            $this->statusRegistry->clear($testId);
        } catch (Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
