<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestErroredSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestFailedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestFinishedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestMarkedIncompleteSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestPassedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestPreparationStartedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestSkippedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestSuiteFinishedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestSuiteStartedSubscriber;
use PHPUnit\Runner\Extension\Extension;
use PHPUnit\Runner\Extension\Facade;
use PHPUnit\Runner\Extension\ParameterCollection;
use PHPUnit\TextUI\Configuration\Configuration as PHPUnitConfiguration;
use Throwable;

final class TelemetryExtension implements Extension
{
    public function bootstrap(
        PHPUnitConfiguration $configuration,
        Facade $facade,
        ParameterCollection $parameters,
    ): void {
        try {
            $config = Configuration::fromParameters($parameters);
            $telemetry = TelemetryFactory::create($config);
            $spanStack = new SpanStack();
            $statusRegistry = new TestStatusRegistry();
            $memoryRegistry = new TestMemoryRegistry();
            $suiteOutcomes = new SuiteOutcomeStack();

            $facade->registerSubscribers(
                new TestSuiteStartedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes),
                new TestSuiteFinishedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes),
                new TestPreparationStartedSubscriber($telemetry, $spanStack, $config, $memoryRegistry),
                new TestPassedSubscriber($statusRegistry),
                new TestFailedSubscriber($statusRegistry),
                new TestErroredSubscriber($statusRegistry),
                new TestSkippedSubscriber($statusRegistry),
                new TestMarkedIncompleteSubscriber($statusRegistry),
                new TestFinishedSubscriber(
                    $telemetry,
                    $spanStack,
                    $config,
                    $statusRegistry,
                    $memoryRegistry,
                    $suiteOutcomes,
                ),
            );
        } catch (Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
