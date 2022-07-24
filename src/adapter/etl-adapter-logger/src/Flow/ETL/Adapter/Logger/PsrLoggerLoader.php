<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

/**
 * @implements Loader<array{logger: LoggerInterface, log_level: string, message: string}>
 */
final class PsrLoggerLoader implements Loader
{
    public function __construct(private LoggerInterface $logger, private string $message, private string $logLevel = LogLevel::DEBUG)
    {
    }

    public function __serialize() : array
    {
        return [
            'logger' => $this->logger,
            'log_level' => $this->logLevel,
            'message' => $this->message,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->logger = $data['logger'];
        $this->logLevel = $data['log_level'];
        $this->message = $data['message'];
    }

    public function load(Rows $rows, FlowContext $context) : void
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
