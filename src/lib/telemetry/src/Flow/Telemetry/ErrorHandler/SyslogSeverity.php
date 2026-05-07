<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * RFC 5424 syslog severity levels.
 */
enum SyslogSeverity : int
{
    case Alert = \LOG_ALERT;
    case Critical = \LOG_CRIT;
    case Debug = \LOG_DEBUG;
    case Emergency = \LOG_EMERG;
    case Error = \LOG_ERR;
    case Info = \LOG_INFO;
    case Notice = \LOG_NOTICE;
    case Warning = \LOG_WARNING;
}
