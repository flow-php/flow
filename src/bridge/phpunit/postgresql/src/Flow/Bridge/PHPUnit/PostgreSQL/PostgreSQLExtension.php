<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL;

use Flow\Bridge\PHPUnit\PostgreSQL\Subscriber\TestPreparationStartedSubscriber;
use Flow\Bridge\PHPUnit\PostgreSQL\Subscriber\TestRunnerFinishedSubscriber;
use Flow\Bridge\PHPUnit\PostgreSQL\Subscriber\TestRunnerStartedSubscriber;
use PHPUnit\Runner\Extension\Extension;
use PHPUnit\Runner\Extension\Facade;
use PHPUnit\Runner\Extension\ParameterCollection;
use PHPUnit\TextUI\Configuration\Configuration as PHPUnitConfiguration;

final class PostgreSQLExtension implements Extension
{
    public function bootstrap(
        PHPUnitConfiguration $configuration,
        Facade $facade,
        ParameterCollection $parameters,
    ): void {
        $facade->registerSubscribers(
            new TestRunnerStartedSubscriber(),
            new TestPreparationStartedSubscriber(),
            new TestRunnerFinishedSubscriber(),
        );
    }
}
