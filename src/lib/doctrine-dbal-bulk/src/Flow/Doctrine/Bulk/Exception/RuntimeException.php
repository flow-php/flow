<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Exception;

final class RuntimeException extends \Exception
{
    public function __construct(string $message, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code = 0, $previous);
    }
}
