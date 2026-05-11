<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Telemetry\ErrorHandler\SyslogFacility;
use Flow\Telemetry\ErrorHandler\SyslogSeverity;

final readonly class UdpSyslogErrorHandlerConfig
{
    public function __construct(
        public string $host,
        public int $port,
        public string $ident,
        public SyslogFacility $facility,
        public SyslogSeverity $severity,
    ) {}
}
