<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class PsrLoggerLoader implements Loader
{
    public function __construct(private LoggerInterface $logger, private string $message, private string $logLevel = LogLevel::DEBUG)
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        /**
         * @psalm-var callable(Row) : void $loader
         */
        $loader = function (Row $row) : void {
            $this->logger->log($this->logLevel, $this->message, $row->toArray());
        };

        $rows->each($loader);
    }
}
