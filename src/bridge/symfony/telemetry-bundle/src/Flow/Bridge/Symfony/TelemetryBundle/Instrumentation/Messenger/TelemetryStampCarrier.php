<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use Flow\Telemetry\Propagation\Carrier;

/**
 * Carrier implementation that wraps a TelemetryStamp.
 *
 * This carrier allows propagators to read from and write to a TelemetryStamp,
 * enabling context propagation through Symfony Messenger messages.
 *
 * @implements Carrier<TelemetryStamp>
 */
final class TelemetryStampCarrier implements Carrier
{
    public function __construct(private TelemetryStamp $stamp = new TelemetryStamp())
    {
    }

    public function get(string $key) : ?string
    {
        return $this->stamp->get($key);
    }

    public function set(string $key, string $value) : static
    {
        $this->stamp = $this->stamp->with($key, $value);

        return $this;
    }

    public function unwrap() : TelemetryStamp
    {
        return $this->stamp;
    }
}
