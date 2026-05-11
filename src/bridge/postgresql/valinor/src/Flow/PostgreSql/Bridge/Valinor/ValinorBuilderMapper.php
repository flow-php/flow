<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor;

use CuyZ\Valinor\Mapper\MappingError;
use CuyZ\Valinor\Mapper\TreeMapper;
use CuyZ\Valinor\MapperBuilder;
use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\RowMapper\Context;

/**
 * @template T of object
 *
 * @implements RowMapper<T>
 */
final readonly class ValinorBuilderMapper implements RowMapper
{
    private TreeMapper $mapper;

    /**
     * @param class-string<T> $class
     */
    public function __construct(
        MapperBuilder $builder,
        private string $class,
    ) {
        $this->mapper = $builder->mapper();
    }

    /**
     * @throws MappingException
     *
     * @return T
     */
    public function map(array $row, Context $context): mixed
    {
        try {
            return $this->mapper->map($this->class, $row);
        } catch (MappingError $error) {
            throw MappingException::mappingFailed($this->class, $error->getMessage(), $error);
        }
    }
}
