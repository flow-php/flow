<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Psr\Log\AbstractLogger;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use Stringable;

use function is_string;
use function str_starts_with;
use function trim;

final class SelectQueryCounter extends AbstractLogger implements LoggerAwareInterface
{
    use LoggerAwareTrait;

    public int $count = 0;

    /**
     * @var array<string>
     */
    public array $queries = [];

    public function __construct()
    {
        $this->logger = new NullLogger();
    }

    public function log(mixed $level, string|Stringable $message, array $context = []): void
    {
        if (!isset($context['sql'])) {
            return;
        }

        // @mago-expect analysis:mixed-assignment
        $sql = $context['sql'];

        if (is_string($sql) || $sql instanceof Stringable) {
            $sqlString = (string) $sql;

            if (str_starts_with(trim($sqlString), 'SELECT')) {
                $this->count++;
                $this->queries[] = $sqlString;
            }
        }
    }

    public function reset(): void
    {
        $this->count = 0;
        $this->queries = [];
    }
}
