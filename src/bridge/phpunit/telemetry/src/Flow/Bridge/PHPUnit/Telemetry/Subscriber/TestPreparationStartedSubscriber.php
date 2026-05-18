<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use PHPUnit\Event\Test\PreparationStarted;
use PHPUnit\Event\Test\PreparationStartedSubscriber;
use Throwable;

final readonly class TestPreparationStartedSubscriber implements PreparationStartedSubscriber
{
    public function __construct(
        private Telemetry $telemetry,
        private SpanStack $spanStack,
        private Configuration $config,
    ) {}

    public function notify(PreparationStarted $event): void
    {
        try {
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
                'test.id' => $test->id(),
                'test.name' => $test->name(),
                'test.class' => $className,
                'test.method' => $methodName,
            ]);

            $this->spanStack->push($span);
        } catch (Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
