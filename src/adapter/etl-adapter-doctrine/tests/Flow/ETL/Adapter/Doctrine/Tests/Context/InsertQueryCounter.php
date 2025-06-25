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

    public function log(mixed $level, string|\Stringable $message, array $context = []) : void
    {
        if (!isset($context['sql'])) {
            return;
        }

        $sql = $context['sql'];

        if (\is_string($sql) || $sql instanceof \Stringable) {
            if (\str_starts_with(\trim((string) $sql), 'INSERT')) {
                $this->count++;
            }
        }
    }

    public function reset() : void
    {
        $this->count = 0;
    }
}
