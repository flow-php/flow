<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

/**
 * Contract for mapping database rows to objects.
 *
 * Implementations can provide various hydration strategies:
 * - Reflection-based mapping
 * - Constructor mapping
 * - Named arguments mapping
 * - Custom attribute-based mapping
 *
 * External libraries (like Symfony Serializer, JMS Serializer, etc.)
 * can implement this interface to integrate with the client.
 */
interface RowMapper
{
    /**
     * Map a database row to an object of the specified class.
     *
     * @template T of object
     *
     * @param class-string<T> $class Target class for mapping
     * @param array<string, mixed> $row Database row as associative array
     *
     * @throws Exception\MappingException When mapping fails
     *
     * @return T Instance of the target class
     */
    public function map(string $class, array $row) : object;
}
