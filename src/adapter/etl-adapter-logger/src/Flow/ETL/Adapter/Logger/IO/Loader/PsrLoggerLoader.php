<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\IO\Loader;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use SensitiveParameter;

final readonly class PsrLoggerLoader implements Loader
{
    public function __construct(
        private LoggerInterface $logger,
        private string $message,
        private string $logLevel = LogLevel::DEBUG,
    ) {}

    public function load(Rows $rows, Backend $backend): void
    {
        if (!$rows->count()) {
            return;
        }

        foreach ($rows->toArray() as $values) {
            $this->logger->log($this->logLevel, $this->message, $values);
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }
}
