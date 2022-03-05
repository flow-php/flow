<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Row\ValueConverter;

/**
 * @implements ValueConverter<array<mixed>>
 * @psalm-immutable
 */
final class AnyToStringCaster implements ValueConverter
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function convert($value) : string
    {
        /** @phpstan-ignore-next-line */
        return (string) $value;
    }
}
