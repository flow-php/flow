<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Row\ValueConverter;

/**
 * @implements ValueConverter<array<mixed>>
 */
final class AnyToIntegerCaster implements ValueConverter
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function convert(mixed $value) : int
    {
        /** @phpstan-ignore-next-line */
        return (int) $value;
    }
}
