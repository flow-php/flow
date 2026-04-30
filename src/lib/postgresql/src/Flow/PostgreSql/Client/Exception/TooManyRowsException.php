<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class TooManyRowsException extends DataAccessException
{
    public function __construct(int $count)
    {
        parent::__construct(\sprintf('Expected at most one row, but %d were returned', $count));
    }
}
