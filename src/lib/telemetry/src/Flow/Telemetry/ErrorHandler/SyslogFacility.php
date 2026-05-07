<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * RFC 5424 syslog facility codes.
 */
enum SyslogFacility : int
{
    case Auth = \LOG_AUTH;
    case Cron = \LOG_CRON;
    case Daemon = \LOG_DAEMON;
    case Kernel = \LOG_KERN;
    case Local0 = \LOG_LOCAL0;
    case Local1 = \LOG_LOCAL1;
    case Local2 = \LOG_LOCAL2;
    case Local3 = \LOG_LOCAL3;
    case Local4 = \LOG_LOCAL4;
    case Local5 = \LOG_LOCAL5;
    case Local6 = \LOG_LOCAL6;
    case Local7 = \LOG_LOCAL7;
    case Lpr = \LOG_LPR;
    case Mail = \LOG_MAIL;
    case News = \LOG_NEWS;
    case Syslog = \LOG_SYSLOG;
    case User = \LOG_USER;
    case Uucp = \LOG_UUCP;
}
