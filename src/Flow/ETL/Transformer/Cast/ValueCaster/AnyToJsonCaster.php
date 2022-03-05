<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\ValueConverter;

/**
 * @implements ValueConverter<array<mixed>>
 * @psalm-immutable
 */
final class AnyToJsonCaster implements ValueConverter
{
    private const JSON_DEPTH = 512;

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
     * @throws \JsonException
     *
     * @return array<mixed>
     */
    public function convert($value) : array
    {
        if (!\is_array($value)) {
            throw new InvalidArgumentException('Only array can be casted to Json, got ' . \gettype($value));
        }

        return (array) \json_decode(\json_encode($value, JSON_THROW_ON_ERROR), true, self::JSON_DEPTH, JSON_THROW_ON_ERROR);
    }
}
