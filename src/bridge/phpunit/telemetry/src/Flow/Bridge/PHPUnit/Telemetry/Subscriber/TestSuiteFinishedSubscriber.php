<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use PHPUnit\Event\TestSuite\Finished;
use PHPUnit\Event\TestSuite\FinishedSubscriber;
use Throwable;

use function str_contains;
use function str_ends_with;

final readonly class TestSuiteFinishedSubscriber implements FinishedSubscriber
{
    public function __construct(
        private Telemetry $telemetry,
        private SpanStack $spanStack,
        private Configuration $config,
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

                $span->end();
                $duration = $span->duration();

                // OTEL spec: instrumentation leaves the status Unset on success.

                if ($this->config->emitMetrics && $duration !== null) {
                    $meter = $this->telemetry->meter('phpunit', $phpunitVersion);

                    $meter->createHistogram('phpunit.suite.duration', 'ms')->record($duration, [
                        'test.suite' => $suiteName,
                    ]);

                    $meter->createCounter('phpunit.suite.test_count')->add($suite->count(), [
                        'test.suite' => $suiteName,
                    ]);
                }

                $tracer->complete($span);

                $this->spanStack->pop();
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
