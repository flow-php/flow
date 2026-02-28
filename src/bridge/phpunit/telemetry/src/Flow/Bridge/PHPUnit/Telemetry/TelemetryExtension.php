<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Bridge\PHPUnit\Telemetry\Subscriber\{TestErroredSubscriber, TestFailedSubscriber, TestFinishedSubscriber, TestMarkedIncompleteSubscriber, TestPassedSubscriber, TestPreparationStartedSubscriber, TestSkippedSubscriber, TestSuiteFinishedSubscriber, TestSuiteStartedSubscriber};
use PHPUnit\Runner\Extension\{Extension, Facade, ParameterCollection};
use PHPUnit\TextUI\Configuration\Configuration as PHPUnitConfiguration;

final class TelemetryExtension implements Extension
{
    public function bootstrap(
        PHPUnitConfiguration $configuration,
        Facade $facade,
        ParameterCollection $parameters,
    ) : void {
        try {
            $config = Configuration::fromParameters($parameters);
            $telemetry = TelemetryFactory::create($config);
            $spanStack = new SpanStack();
            $statusRegistry = new TestStatusRegistry();

            $facade->registerSubscribers(
                new TestSuiteStartedSubscriber($telemetry, $spanStack, $config),
                new TestSuiteFinishedSubscriber($telemetry, $spanStack, $config),
                new TestPreparationStartedSubscriber($telemetry, $spanStack, $config),
                new TestPassedSubscriber($statusRegistry),
                new TestFailedSubscriber($statusRegistry),
                new TestErroredSubscriber($statusRegistry),
                new TestSkippedSubscriber($statusRegistry),
                new TestMarkedIncompleteSubscriber($statusRegistry),
                new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry),
            );
        } catch (\Throwable) {
            // Silent failure - telemetry must never break tests
        }
    }
}
