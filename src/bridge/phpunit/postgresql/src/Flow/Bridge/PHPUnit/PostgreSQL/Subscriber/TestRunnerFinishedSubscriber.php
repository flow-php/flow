<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Subscriber;

use Flow\Bridge\PHPUnit\PostgreSQL\StaticClient;
use PHPUnit\Event\TestRunner\{Finished, FinishedSubscriber};

final readonly class TestRunnerFinishedSubscriber implements FinishedSubscriber
{
    public function notify(Finished $event) : void
    {
        StaticClient::rollBack();
        StaticClient::disable();
        StaticClient::closeAll();
    }
}
