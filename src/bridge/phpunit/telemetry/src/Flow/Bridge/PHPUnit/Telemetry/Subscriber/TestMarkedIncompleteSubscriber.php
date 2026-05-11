<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\TestStatusRegistry;
use PHPUnit\Event\Test\MarkedIncomplete;
use PHPUnit\Event\Test\MarkedIncompleteSubscriber;

final readonly class TestMarkedIncompleteSubscriber implements MarkedIncompleteSubscriber
{
    public function __construct(
        private TestStatusRegistry $statusRegistry,
    ) {}

    public function notify(MarkedIncomplete $event): void
    {
        $this->statusRegistry->setStatus($event->test()->id(), 'incomplete', $event->throwable()->message());
    }
}
