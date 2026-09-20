<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Psr\Log\AbstractLogger;
use Stringable;

final class CommitCounter extends AbstractLogger
{
    public int $count = 0;

    public function log(mixed $level, string|Stringable $message, array $context = []): void
    {
        if ((string) $message === 'Committing transaction') {
            $this->count++;
        }
    }
}
