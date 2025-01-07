<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Psr\Log\{AbstractLogger, LoggerAwareInterface, LoggerAwareTrait, NullLogger};

final class InsertQueryCounter extends AbstractLogger implements LoggerAwareInterface
{
    use LoggerAwareTrait;

    public int $count = 0;

    public function __construct()
    {
        $this->logger = new NullLogger();
    }

    public function log($level, $message, array $context = []) : void
    {
        if (!isset($context['sql'])) {
            return;
        }

        if (\str_starts_with(\trim((string) $context['sql']), 'INSERT')) {
            $this->count++;
        }
    }
}
