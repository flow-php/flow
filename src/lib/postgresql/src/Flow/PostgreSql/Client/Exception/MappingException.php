<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class MappingException extends ClientException
{
    public static function factoryMethodNotFound(string $class, string $method) : self
    {
        return new self(\sprintf('Static factory method "%s::%s()" does not exist', $class, $method));
    }

    public static function factoryMethodNotPublic(string $class, string $method) : self
    {
        return new self(\sprintf('Factory method "%s::%s()" must be declared public', $class, $method));
    }

    public static function factoryMethodNotStatic(string $class, string $method) : self
    {
        return new self(\sprintf('Factory method "%s::%s()" must be declared static', $class, $method));
    }

    public static function mappingFailed(string $class, string $reason, ?\Throwable $previous = null) : self
    {
        return new self(\sprintf('Failed to map row to "%s": %s', $class, $reason), 0, $previous);
    }

    public static function propertyNotFound(string $class, string $property) : self
    {
        return new self(\sprintf('Property "%s" not found on class "%s"', $property, $class));
    }
}
