<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * Backed counterpart of PHP's error_log() $message_type argument.
 */
enum ErrorLogMessageType : int
{
    case Email = 1;
    case File = 3;
    case OperatingSystem = 0;
    case Sapi = 4;
}
