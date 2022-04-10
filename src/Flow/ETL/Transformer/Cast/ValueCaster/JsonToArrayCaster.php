<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\ValueConverter;

/**
 * @implements ValueConverter<array<mixed>>
 * @psalm-immutable
 */
final class JsonToArrayCaster implements ValueConverter
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    /**
     * @param mixed $value
     *
     * @throws InvalidArgumentException
     *
     * @return array<mixed>
     */
    public function convert(mixed $value) : array
    {
        if (!\is_string($value)) {
            throw new InvalidArgumentException('Only json string can be casted to Array, got ' . \gettype($value));
        }

        return (array) \json_decode($value, true, JSON_THROW_ON_ERROR, JSON_THROW_ON_ERROR);
    }
}
