<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Exception;

use Symfony\Component\Cache\Exception\InvalidArgumentException;

final class CacheException extends InvalidArgumentException
{
    public static function unexpectedRowShape(string $field, string $actualType): self
    {
        return new self(\sprintf('Unexpected cache row shape: field "%s" must be string, got %s', $field, $actualType));
    }
}
