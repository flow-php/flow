<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor;

use CuyZ\Valinor\Mapper\{MappingError, TreeMapper};
use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper;

/**
 * @template T of object
 *
 * @implements RowMapper<T>
 */
final readonly class ValinorTreeMapper implements RowMapper
{
    /**
     * @param class-string<T> $class
     */
    public function __construct(
        private TreeMapper $mapper,
        private string $class,
    ) {
    }

    /**
     * @throws MappingException
     *
     * @return T
     */
    public function map(array $row) : mixed
    {
        try {
            return $this->mapper->map($this->class, $row);
        } catch (MappingError $error) {
            throw MappingException::mappingFailed($this->class, $error->getMessage(), $error);
        }
    }
}
