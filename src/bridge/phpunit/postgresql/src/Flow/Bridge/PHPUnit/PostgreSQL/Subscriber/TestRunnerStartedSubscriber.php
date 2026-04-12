<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Subscriber;

use Flow\Bridge\PHPUnit\PostgreSQL\StaticClient;
use PHPUnit\Event\TestRunner\{Started, StartedSubscriber};

final readonly class TestRunnerStartedSubscriber implements StartedSubscriber
{
    public function notify(Started $event) : void
    {
        StaticClient::enable();
    }
}
