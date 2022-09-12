<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Row\ValueConverter;

/**
 * @implements ValueConverter<array<mixed>>
 *
 * @psalm-immutable
 */
final class AnyToArrayCaster implements ValueConverter
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
     * @return array<mixed>
     */
    public function convert(mixed $value) : array
    {
        return (array) $value;
    }
}
