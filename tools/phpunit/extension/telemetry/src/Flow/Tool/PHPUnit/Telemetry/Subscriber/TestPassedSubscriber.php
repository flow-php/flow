<?php

declare(strict_types=1);

namespace Flow\Tool\PHPUnit\Telemetry\Subscriber;

use Flow\Tool\PHPUnit\Telemetry\TestStatusRegistry;
use PHPUnit\Event\Test\{Passed, PassedSubscriber};

final readonly class TestPassedSubscriber implements PassedSubscriber
{
    public function __construct(
        private TestStatusRegistry $statusRegistry,
    ) {
    }

    public function notify(Passed $event) : void
    {
        $this->statusRegistry->setStatus($event->test()->id(), 'passed');
    }
}
