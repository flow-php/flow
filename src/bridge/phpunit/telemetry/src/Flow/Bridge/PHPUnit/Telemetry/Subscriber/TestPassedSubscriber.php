<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\TestStatusRegistry;
use PHPUnit\Event\Test\Passed;
use PHPUnit\Event\Test\PassedSubscriber;

final readonly class TestPassedSubscriber implements PassedSubscriber
{
    public function __construct(
        private TestStatusRegistry $statusRegistry,
    ) {}

    public function notify(Passed $event): void
    {
        $this->statusRegistry->setStatus($event->test()->id(), 'passed');
    }
}
