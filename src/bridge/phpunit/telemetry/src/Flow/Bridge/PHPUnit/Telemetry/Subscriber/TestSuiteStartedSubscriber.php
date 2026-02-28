<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\{Configuration, SpanStack};
use Flow\Telemetry\{PackageVersion, Telemetry};
use PHPUnit\Event\TestSuite\{Started, StartedSubscriber};

final readonly class TestSuiteStartedSubscriber implements StartedSubscriber
{
    public function __construct(
        private Telemetry $telemetry,
        private SpanStack $spanStack,
        private Configuration $config,
    ) {
    }

    public function notify(Started $event) : void
    {
        try {
            $suite = $event->testSuite();
            $suiteName = $suite->name();

            $isRoot = $suiteName === ''
                || $suiteName === 'PHPUnit Test Suite'
                || $suiteName === 'CLI Arguments'
                || \str_ends_with((string) $suiteName, '.xml')
                || \str_ends_with((string) $suiteName, '.xml.dist');

            $isTestCase = \str_contains((string) $suiteName, '\\') || \str_contains((string) $suiteName, '::');

            if (!$isRoot && $isTestCase && !$this->config->emitTestCaseSpans) {
                return;
            }

            $tracer = $this->telemetry->tracer('phpunit', PackageVersion::get('phpunit/phpunit'));

            $span = $tracer->span(
                $isRoot ? 'Test Suite Run' : $suite->name(),
                attributes: [
                    'test.suite' => $suite->name(),
                    'test.suite.test_count' => $suite->count(),
                    'test.suite.is_root' => $isRoot,
                ],
            );

            $this->spanStack->setSuiteSpan($suite->name(), $span);
            $this->spanStack->push($span);
        } catch (\Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
