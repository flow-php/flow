<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel;

use Flow\Bridge\Symfony\TelemetryBundle\Attribute\WithTelemetryChannel;
use Flow\Telemetry\Logger\Logger;

#[WithTelemetryChannel('events')]
final class NativeChannelConsumer
{
    public function __construct(
        public readonly Logger $logger,
    ) {}
}
