<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\PHPUnitTelemetryAttributes;
use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\SuiteOutcomeStack;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Telemetry;
use PHPUnit\Event\TestSuite\Started;
use PHPUnit\Event\TestSuite\StartedSubscriber;
use Throwable;

use function str_contains;
use function str_ends_with;

final readonly class TestSuiteStartedSubscriber implements StartedSubscriber
{
    public function __construct(
        private Telemetry $telemetry,
        private SpanStack $spanStack,
        private Configuration $config,
        private SuiteOutcomeStack $suiteOutcomes,
    ) {}

    public function notify(Started $event): void
    {
        try {
            $suite = $event->testSuite();
            $suiteName = $suite->name();

            $isRoot =
                $suiteName === 'PHPUnit Test Suite'
                || $suiteName === 'CLI Arguments'
                || str_ends_with($suiteName, '.xml')
                || str_ends_with($suiteName, '.xml.dist');

            $isTestCase = str_contains($suiteName, '\\') || str_contains($suiteName, '::');

            if (!$isRoot && $isTestCase && !$this->config->emitTestCaseSpans) {
                return;
            }

            $tracer = $this->telemetry->tracer('phpunit', PackageVersion::get('phpunit/phpunit'));

            $span = $tracer->span($isRoot ? 'Test Suite Run' : $suite->name(), attributes: [
                SemConvAttributes::TEST_SUITE_NAME => $suite->name(),
                PHPUnitTelemetryAttributes::ATTR_SUITE_TEST_COUNT => $suite->count(),
                PHPUnitTelemetryAttributes::ATTR_SUITE_IS_ROOT => $isRoot,
            ]);

            // activated: suites nest, and test spans must nest under their suite
            $this->spanStack->setSuiteSpan($suite->name(), $span);
            $this->spanStack->push($span, $tracer->activate($span));
            $this->suiteOutcomes->push();
        } catch (Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
