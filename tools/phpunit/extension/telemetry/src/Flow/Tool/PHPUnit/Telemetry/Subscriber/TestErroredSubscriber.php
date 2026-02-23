<?php

declare(strict_types=1);

namespace Flow\Tool\PHPUnit\Telemetry\Subscriber;

use Flow\Tool\PHPUnit\Telemetry\TestStatusRegistry;
use PHPUnit\Event\Test\{Errored, ErroredSubscriber};

final readonly class TestErroredSubscriber implements ErroredSubscriber
{
    public function __construct(
        private TestStatusRegistry $statusRegistry,
    ) {
    }

    public function notify(Errored $event) : void
    {
        $this->statusRegistry->setStatus($event->test()->id(), 'errored', $event->throwable()->message());
    }
}
