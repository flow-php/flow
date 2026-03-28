<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class MappingException extends ClientException
{
    public static function mappingFailed(string $class, string $reason) : self
    {
        return new self(\sprintf('Failed to map row to "%s": %s', $class, $reason));
    }

    public static function propertyNotFound(string $class, string $property) : self
    {
        return new self(\sprintf('Property "%s" not found on class "%s"', $property, $class));
    }
}
