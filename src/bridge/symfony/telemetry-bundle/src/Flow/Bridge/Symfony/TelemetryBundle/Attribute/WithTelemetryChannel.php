<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Attribute;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS)]
final class WithTelemetryChannel
{
    public function __construct(
        public string $channel,
    ) {}
}
