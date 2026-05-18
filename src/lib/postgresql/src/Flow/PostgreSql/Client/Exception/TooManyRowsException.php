<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

use function sprintf;

final class TooManyRowsException extends DataAccessException
{
    public function __construct(int $count)
    {
        parent::__construct(sprintf('Expected at most one row, but %d were returned', $count));
    }
}
