<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\RowMapper;

use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper;
use Flow\Types\Type;

/**
 * Maps database rows to typed values using flow-php/types.
 *
 * When $next is null, the mapper returns the value produced by $type->cast().
 * When $next is provided, the cast result is forwarded to $next->map() and its
 * return value becomes the mapper's output.
 *
 * @template TType
 * @template TNext = never
 *
 * @implements RowMapper<TNext|TType>
 */
final readonly class TypeMapper implements RowMapper
{
    /**
     * @param Type<TType> $type
     * @param null|RowMapper<TNext> $next
     */
    public function __construct(
        private Type $type,
        private ?RowMapper $next = null,
    ) {
    }

    public function map(array $row) : mixed
    {
        try {
            $result = $this->type->cast($row);
        } catch (\Throwable $e) {
            throw new MappingException('Failed to map database row to type: ' . $e->getMessage(), previous: $e);
        }

        return $this->forward($result, $this->next);
    }

    /**
     * @template TForwardNext
     *
     * @param TType $value
     * @param null|RowMapper<TForwardNext> $next
     *
     * @return ($next is null ? TType : TForwardNext)
     */
    private function forward(mixed $value, ?RowMapper $next) : mixed
    {
        if ($next === null) {
            return $value;
        }

        /** @var array<string, mixed> $value */
        return $next->map($value);
    }
}
