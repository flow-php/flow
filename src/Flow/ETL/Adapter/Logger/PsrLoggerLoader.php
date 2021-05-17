<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger;

use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class PsrLoggerLoader implements Loader
{
    private LoggerInterface $logger;

    private string $logLevel;

    private string $message;

    public function __construct(LoggerInterface $logger, string $message, string $logLeve = LogLevel::DEBUG)
    {
        $this->logger = $logger;
        $this->logLevel = $logLeve;
        $this->message = $message;
    }

    public function load(Rows $rows) : void
    {
        /**
         * @psalm-var pure-callable(Row) : void $loader
         */
        $loader = function (Row $row) : void {
            $this->logger->log($this->logLevel, $this->message, $row->toArray());
        };

        /** @psalm-suppress UnusedMethodCall */
        $rows->each($loader);
    }
}
