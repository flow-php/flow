<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Telemetry\ErrorHandler\SyslogFacility;
use Flow\Telemetry\ErrorHandler\SyslogSeverity;

final readonly class SyslogErrorHandlerConfig
{
    public function __construct(
        public string $ident,
        public SyslogFacility $facility,
        public int $logOpts,
        public SyslogSeverity $severity,
    ) {}
}
