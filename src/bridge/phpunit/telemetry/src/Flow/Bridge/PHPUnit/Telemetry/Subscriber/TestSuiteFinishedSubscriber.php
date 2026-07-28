<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\SuiteOutcomeStack;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Telemetry;
use PHPUnit\Event\TestSuite\Finished;
use PHPUnit\Event\TestSuite\FinishedSubscriber;
use Throwable;

use function str_contains;
use function str_ends_with;

final readonly class TestSuiteFinishedSubscriber implements FinishedSubscriber
{
    /**
     * Suites run longer than single tests, so the semconv-advised seconds boundaries are extended
     * up to 10 minutes.
     */
    private const array DURATION_BOUNDARIES = [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0];

    public function __construct(
        private Telemetry $telemetry,
        private SpanStack $spanStack,
        private Configuration $config,
        private SuiteOutcomeStack $suiteOutcomes,
    ) {}

    public function notify(Finished $event): void
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

            $span = $this->spanStack->getSuiteSpan($suiteName);

            if ($span !== null) {
                $phpunitVersion = PackageVersion::get('phpunit/phpunit');
                $tracer = $this->telemetry->tracer('phpunit', $phpunitVersion);

                $runStatus = $this->suiteOutcomes->pop();

                if ($runStatus !== null) {
                    $span->setAttribute(SemConvAttributes::TEST_SUITE_RUN_STATUS, $runStatus);
                }

                $span->end();
                $duration = $span->duration();

                // OTEL spec: instrumentation leaves the status Unset on success.

                if ($this->config->emitMetrics && $duration !== null) {
                    $meter = $this->telemetry->meter('phpunit', $phpunitVersion);

                    $meter->createHistogram(
                        'flow.phpunit.suite.duration',
                        's',
                        'Duration of a test suite run',
                        self::DURATION_BOUNDARIES,
                    )->record($duration / 1_000, [
                        SemConvAttributes::TEST_SUITE_NAME => $suiteName,
                    ]);

                    $meter->createCounter('flow.phpunit.suite.test_count', '{test}')->add($suite->count(), [
                        SemConvAttributes::TEST_SUITE_NAME => $suiteName,
                    ]);
                }

                // pop first: it detaches the context scope, which must happen before the span completes
                $this->spanStack->pop();
                $tracer->complete($span);

                $this->spanStack->removeSuiteSpan($suiteName);
            }

            if ($isRoot) {
                $this->telemetry->shutdown();
            }
        } catch (Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
