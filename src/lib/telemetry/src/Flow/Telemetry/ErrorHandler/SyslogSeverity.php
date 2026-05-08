<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * RFC 5424 syslog severity levels.
 */
enum SyslogSeverity : int
{
    case Alert = 1;
    case Critical = 2;
    case Debug = 7;
    case Emergency = 0;
    case Error = 3;
    case Info = 6;
    case Notice = 5;
    case Warning = 4;
}
