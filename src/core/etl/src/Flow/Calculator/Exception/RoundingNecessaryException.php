<?php

declare(strict_types=1);

namespace Flow\Calculator\Exception;

use Throwable;

final class RoundingNecessaryException extends Exception
{
    public function __construct(string $message = '', int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
