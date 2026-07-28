<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\PHPUnitTelemetryAttributes;
use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\TestMemoryRegistry;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Telemetry;
use PHPUnit\Event\Test\PreparationStarted;
use PHPUnit\Event\Test\PreparationStartedSubscriber;
use Throwable;

use function memory_get_usage;
use function memory_reset_peak_usage;

final readonly class TestPreparationStartedSubscriber implements PreparationStartedSubscriber
{
    public function __construct(
        private Telemetry $telemetry,
        private SpanStack $spanStack,
        private Configuration $config,
        private TestMemoryRegistry $memoryRegistry,
    ) {}

    public function notify(PreparationStarted $event): void
    {
        try {
            if ($this->config->emitTestSpans || $this->config->emitMetrics) {
                memory_reset_peak_usage();
                $this->memoryRegistry->setStart($event->test()->id(), memory_get_usage($this->config->memoryRealUsage));
            }

            if (!$this->config->emitTestSpans) {
                return;
            }

            $test = $event->test();
            $tracer = $this->telemetry->tracer('phpunit', PackageVersion::get('phpunit/phpunit'));

            $className = '';
            $methodName = '';

            if ($test->isTestMethod()) {
                $testMethod = $test;
                $className = $testMethod->className();
                $methodName = $testMethod->methodName();
            }

            $span = $tracer->span($test->name(), attributes: [
                PHPUnitTelemetryAttributes::ATTR_TEST_ID => $test->id(),
                SemConvAttributes::TEST_CASE_NAME => $test->name(),
                PHPUnitTelemetryAttributes::ATTR_TEST_CLASS => $className,
                PHPUnitTelemetryAttributes::ATTR_TEST_METHOD => $methodName,
            ]);

            // activated: spans the test itself emits must nest under the test span
            $this->spanStack->push($span, $tracer->activate($span));
        } catch (Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
