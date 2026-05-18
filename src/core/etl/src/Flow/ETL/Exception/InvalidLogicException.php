<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use function sprintf;

final class InvalidLogicException extends Exception
{
    public static function because(string $format, float|int|string ...$parameters): self
    {
        return new self(sprintf($format, ...$parameters));
    }
}
