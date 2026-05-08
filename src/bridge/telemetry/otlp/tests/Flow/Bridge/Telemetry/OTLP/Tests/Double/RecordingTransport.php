<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Double;

use Flow\Bridge\Telemetry\OTLP\Transport\Transport;
use Flow\Telemetry\Signal\Signals;

final class RecordingTransport implements Transport
{
    public ?\Throwable $sendException = null;

    /** @var list<Signals> */
    public array $sent = [];

    public int $shutdownCalls = 0;

    public ?\Throwable $shutdownException = null;

    public function send(Signals $signal) : void
    {
        $this->sent[] = $signal;

        if ($this->sendException !== null) {
            throw $this->sendException;
        }
    }

    public function shutdown() : void
    {
        $this->shutdownCalls++;

        if ($this->shutdownException !== null) {
            throw $this->shutdownException;
        }
    }
}
