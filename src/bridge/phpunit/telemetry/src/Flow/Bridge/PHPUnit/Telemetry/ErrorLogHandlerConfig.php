<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Telemetry\ErrorHandler\ErrorLogMessageType;

final readonly class ErrorLogHandlerConfig
{
    public function __construct(
        public ErrorLogMessageType $messageType,
        public bool $expandNewlines,
        public string $messagePrefix,
    ) {}
}
