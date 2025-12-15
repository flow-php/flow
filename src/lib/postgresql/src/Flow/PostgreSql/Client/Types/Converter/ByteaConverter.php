<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_string;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<string>
 */
final class ByteaConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_string();
    }

    public function supportedTypes() : array
    {
        return [PostgreSqlType::BYTEA];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (!\is_string($value)) {
            return '';
        }

        return $value;
    }

    public function toPhp(string $value, PostgreSqlType $type) : string
    {
        if (\str_starts_with($value, '\\x')) {
            $hex = \hex2bin(\substr($value, 2));

            if ($hex === false) {
                return $value;
            }

            return $hex;
        }

        return \pg_unescape_bytea($value);
    }
}
