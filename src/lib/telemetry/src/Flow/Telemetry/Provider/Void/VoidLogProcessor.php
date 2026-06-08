<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogSink;

/**
 * No-op log processor that discards all data.
 */
final readonly class VoidLogProcessor implements LogSink
{
    public function flush(): bool
    {
        return true;
    }

    public function process(LogEntry $entry): void {}

    public function shutdown(): void {}
}
