<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel;

use Psr\Log\LoggerInterface;

final class NamedArgumentConsumer
{
    public function __construct(
        public readonly LoggerInterface $eventsLogger,
    ) {}
}
