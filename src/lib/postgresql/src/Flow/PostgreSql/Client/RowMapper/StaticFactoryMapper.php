<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\RowMapper;

use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper;

/**
 * Maps database rows to objects using a user-provided public static factory method.
 *
 * The factory method must:
 * - be public,
 * - be static,
 * - accept a single parameter: array<string, mixed> $row,
 * - return an instance of $class (or a subclass).
 *
 * If the factory needs access to the mapping Context (sql/parameters/client/catalog/user-data),
 * implement Flow\PostgreSql\Client\RowMapper directly instead — this mapper intentionally hides Context.
 *
 * @template T of object
 *
 * @implements RowMapper<T>
 */
final readonly class StaticFactoryMapper implements RowMapper
{
    /**
     * @param class-string<T> $class
     * @param non-empty-string $method
     *
     * @throws MappingException
     */
    public function __construct(
        private string $class,
        private string $method,
    ) {
        if (!\class_exists($this->class)) {
            throw MappingException::mappingFailed($this->class, 'Class does not exist');
        }

        if (!\method_exists($this->class, $this->method)) {
            throw MappingException::factoryMethodNotFound($this->class, $this->method);
        }

        $reflection = new \ReflectionMethod($this->class, $this->method);

        if (!$reflection->isStatic()) {
            throw MappingException::factoryMethodNotStatic($this->class, $this->method);
        }

        if (!$reflection->isPublic()) {
            throw MappingException::factoryMethodNotPublic($this->class, $this->method);
        }
    }

    /**
     * @return T
     */
    public function map(array $row, Context $context): object
    {
        try {
            /** @var T */
            return $this->class::{$this->method}($row);
        } catch (\Throwable $e) {
            throw MappingException::mappingFailed($this->class, $e->getMessage(), $e);
        }
    }
}
