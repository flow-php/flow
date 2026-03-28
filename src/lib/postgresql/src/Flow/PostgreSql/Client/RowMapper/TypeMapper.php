<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\RowMapper;

use Flow\PostgreSql\Client\RowMapper;
use Flow\Types\Type;

/**
 * Maps database rows to typed arrays using flow-php/types.
 *
 * @template T
 *
 * @implements RowMapper<T>
 */
final readonly class TypeMapper implements RowMapper
{
    /**
     * @param Type<T> $type
     */
    public function __construct(private Type $type)
    {
    }

    /**
     * @return T
     */
    public function map(array $row) : mixed
    {
        return $this->type->assert($row);
    }
}
