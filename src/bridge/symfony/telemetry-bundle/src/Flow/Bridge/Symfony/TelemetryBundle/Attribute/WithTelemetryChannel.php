<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Attribute;

use Attribute;

/**
 * Routes a service's logger to a named Flow telemetry channel.
 *
 * Applying this attribute autoconfigures the service with the
 * "flow.telemetry.channel" tag, so its autowired PSR-3 LoggerInterface
 * resolves to the channel's logger (scope) instead of the default one:
 *
 * ```php
 * #[WithTelemetryChannel('events')]
 * final class OrderSubscriber
 * {
 *     public function __construct(private LoggerInterface $logger) {}
 * }
 * ```
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class WithTelemetryChannel
{
    public function __construct(
        public string $channel,
    ) {}
}
