<?php

declare(strict_types=1);

namespace Flow\Tool\PHPUnit\Telemetry\Subscriber;

use Flow\Tool\PHPUnit\Telemetry\TestStatusRegistry;
use PHPUnit\Event\Test\{Failed, FailedSubscriber};

final readonly class TestFailedSubscriber implements FailedSubscriber
{
    public function __construct(
        private TestStatusRegistry $statusRegistry,
    ) {
    }

    public function notify(Failed $event) : void
    {
        $this->statusRegistry->setStatus($event->test()->id(), 'failed', $event->throwable()->message());
    }
}
