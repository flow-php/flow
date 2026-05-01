<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class NoResultException extends DataAccessException
{
    public function __construct(string $message = 'Expected at least one row, but none were returned')
    {
        parent::__construct($message);
    }
}
