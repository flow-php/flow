<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

use function get_debug_type;
use function sprintf;
use function strlen;
use function substr;

final class ValueConversionException extends ClientException
{
    public static function ambiguousArrayType(): self
    {
        return new self(
            'Array parameters require explicit type specification. Use typed($array, ValueType::INT4_ARRAY) or similar to specify the target array type.',
        );
    }

    public static function cannotConvert(mixed $value, string $targetType): self
    {
        $valueType = get_debug_type($value);

        return new self(sprintf('Cannot convert value of type "%s" to "%s"', $valueType, $targetType));
    }

    public static function invalidArrayLiteral(string $literal, string $reason): self
    {
        $preview = strlen($literal) > 20 ? substr($literal, 0, 20) . '...' : $literal;

        return new self(sprintf('Invalid PostgreSQL array literal (%s): %s', $reason, $preview));
    }

    public static function invalidByteaData(string $value): self
    {
        $preview = strlen($value) > 20 ? substr($value, 0, 20) . '...' : $value;

        return new self(sprintf('Invalid bytea hex data: %s', $preview));
    }

    public static function unsupportedOid(int $oid): self
    {
        return new self(sprintf('No type converter registered for PostgreSQL OID %d', $oid));
    }
}
