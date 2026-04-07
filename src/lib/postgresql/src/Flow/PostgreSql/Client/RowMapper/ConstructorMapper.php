<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\RowMapper;

use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper;

/**
 * Maps database rows directly to constructor parameters.
 *
 * Requirements:
 * - SQL column names must match constructor parameter names exactly (1:1)
 * - Class must have a constructor with all parameters
 * - Use SQL aliases if column names differ from parameter names:
 *   SELECT created_at AS createdAt FROM users
 *
 * @template T of object
 *
 * @implements RowMapper<T>
 */
final readonly class ConstructorMapper implements RowMapper
{
    /**
     * @param class-string<T> $class
     */
    public function __construct(private string $class)
    {
    }

    /**
     * @return T
     */
    public function map(array $row) : object
    {
        if (!\class_exists($this->class)) {
            throw MappingException::mappingFailed($this->class, 'Class does not exist');
        }

        $reflection = new \ReflectionClass($this->class);

        $constructor = $reflection->getConstructor();

        if ($constructor === null) {
            throw MappingException::mappingFailed($this->class, 'Class has no constructor');
        }

        $args = [];

        foreach ($constructor->getParameters() as $param) {
            $paramName = $param->getName();

            if (\array_key_exists($paramName, $row)) {
                $args[$paramName] = $row[$paramName];
            } elseif ($param->isDefaultValueAvailable()) {
                continue;
            } elseif ($param->allowsNull()) {
                $args[$paramName] = null;
            } else {
                throw MappingException::propertyNotFound($this->class, $paramName);
            }
        }

        try {
            return $reflection->newInstanceArgs($args);
        } catch (\Throwable $e) {
            throw MappingException::mappingFailed($this->class, $e->getMessage());
        }
    }
}
