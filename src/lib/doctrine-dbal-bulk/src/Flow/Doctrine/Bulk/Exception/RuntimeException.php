<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Exception;

use Exception;
use Throwable;

final class RuntimeException extends Exception
{
    public function __construct(string $message, ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}
