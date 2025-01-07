<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger;

use Flow\ETL\{FlowContext, Loader, Row, Rows};
use Psr\Log\{LogLevel, LoggerInterface};

final readonly class PsrLoggerLoader implements Loader
{
    public function __construct(private LoggerInterface $logger, private string $message, private string $logLevel = LogLevel::DEBUG)
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $loader = function (Row $row) : void {
            $this->logger->log($this->logLevel, $this->message, $row->toArray());
        };

        $rows->each($loader);
    }
}
