<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Row\ValueConverter;

/**
 * @implements ValueConverter<array<mixed>>
 *
 * @psalm-immutable
 */
final class AnyToBooleanCaster implements ValueConverter
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function convert(mixed $value) : bool
    {
        if (\is_string($value)) {
            if (\trim(\strtolower($value)) === 'true') {
                return true;
            }

            if (\trim(\strtolower($value)) === 'false') {
                return false;
            }

            if (\trim(\strtolower($value)) === '1') {
                return true;
            }

            if (\trim(\strtolower($value)) === '0') {
                return false;
            }
        }

        return (bool) $value;
    }
}
