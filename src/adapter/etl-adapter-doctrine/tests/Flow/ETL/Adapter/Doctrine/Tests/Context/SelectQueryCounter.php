<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Psr\Log\{AbstractLogger, LoggerAwareInterface, LoggerAwareTrait, NullLogger};

final class SelectQueryCounter extends AbstractLogger implements LoggerAwareInterface
{
    use LoggerAwareTrait;

    public int $count = 0;

    public array $queries = [];

    public function __construct()
    {
        $this->logger = new NullLogger();
    }

    public function log(mixed $level, string|\Stringable $message, array $context = []) : void
    {
        if (!isset($context['sql'])) {
            return;
        }

        if (\str_starts_with(\trim((string) $context['sql']), 'SELECT')) {
            $this->count++;
            $this->queries[] = $context['sql'];
        }
    }

    public function reset() : void
    {
        $this->count = 0;
        $this->queries = [];
    }
}
