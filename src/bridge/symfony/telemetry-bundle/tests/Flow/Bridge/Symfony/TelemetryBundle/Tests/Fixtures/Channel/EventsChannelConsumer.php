<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel;

use Flow\Bridge\Symfony\TelemetryBundle\Attribute\WithTelemetryChannel;
use Psr\Log\LoggerInterface;

#[WithTelemetryChannel('events')]
final class EventsChannelConsumer
{
    public function __construct(
        public readonly LoggerInterface $logger,
    ) {}
}
