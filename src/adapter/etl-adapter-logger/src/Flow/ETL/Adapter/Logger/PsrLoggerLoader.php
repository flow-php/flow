<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Loader, Row, Rows};
use Psr\Log\{LogLevel, LoggerInterface};

final readonly class PsrLoggerLoader implements Loader
{
    public function __construct(private LoggerInterface $logger, private string $message, private string $logLevel = LogLevel::DEBUG)
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            $loader = function (Row $row) : void {
                $this->logger->log($this->logLevel, $this->message, $row->toArray());
            };

            $rows->each($loader);

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (\Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }
}
