<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\TestStatusRegistry;
use PHPUnit\Event\Test\{Skipped, SkippedSubscriber};

final readonly class TestSkippedSubscriber implements SkippedSubscriber
{
    public function __construct(
        private TestStatusRegistry $statusRegistry,
    ) {
    }

    public function notify(Skipped $event) : void
    {
        $this->statusRegistry->setStatus($event->test()->id(), 'skipped', $event->message());
    }
}
