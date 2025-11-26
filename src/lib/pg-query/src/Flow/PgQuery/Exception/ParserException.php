<?php

declare(strict_types=1);

namespace Flow\PgQuery\Exception;

final class ParserException extends \RuntimeException
{
    public function __construct(string $message, public readonly ?int $cursorPosition = null)
    {
        parent::__construct($message);
    }
}
