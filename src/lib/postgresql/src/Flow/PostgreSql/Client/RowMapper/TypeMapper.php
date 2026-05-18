<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\RowMapper;

use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper;
use Flow\Types\Type;
use Throwable;

/**
 * Maps database rows to typed values using flow-php/types.
 *
 * When $next is null, the mapper returns the value produced by $type->cast().
 * When $next is provided, the cast result is forwarded to $next->map() and its
 * return value becomes the mapper's output.
 *
 * @template TType
 * @template TOut
 *
 * @implements RowMapper<TOut>
 */
final readonly class TypeMapper implements RowMapper
{
    /**
     * @param Type<TType> $type
     * @param null|RowMapper<TOut> $next
     */
    public function __construct(
        private Type $type,
        private ?RowMapper $next = null,
    ) {}

    public function map(array $row, Context $context): mixed
    {
        try {
            $result = $this->type->cast($row);
        } catch (Throwable $e) {
            throw new MappingException('Failed to map database row to type: ' . $e->getMessage(), previous: $e);
        }

        if ($this->next === null) {
            /** @var TOut $result */
            return $result;
        }

        /** @var array<string, mixed> $result */
        return $this->next->map($result, $context);
    }
}
