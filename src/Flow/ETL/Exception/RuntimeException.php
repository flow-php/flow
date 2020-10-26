<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

final class RuntimeException extends Exception
{
    /**
     * @param float|int|string ...$parameters
     */
    public static function because(string $format, ...$parameters) : self
    {
        return new self(\sprintf($format, ...$parameters));
    }
}
