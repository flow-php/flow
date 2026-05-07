<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * RFC 5424 syslog facility codes.
 */
enum SyslogFacility : int
{
    case Auth = 32;
    case Cron = 72;
    case Daemon = 24;
    case Kernel = 0;
    case Local0 = 128;
    case Local1 = 136;
    case Local2 = 144;
    case Local3 = 152;
    case Local4 = 160;
    case Local5 = 168;
    case Local6 = 176;
    case Local7 = 184;
    case Lpr = 48;
    case Mail = 16;
    case News = 56;
    case Syslog = 40;
    case User = 8;
    case Uucp = 64;
}
