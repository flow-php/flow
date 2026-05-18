<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use function sprintf;

class InvalidArgumentException extends Exception
{
    public static function because(string $format, float|int|string ...$parameters): self
    {
        return new self(sprintf($format, ...$parameters));
    }
}
