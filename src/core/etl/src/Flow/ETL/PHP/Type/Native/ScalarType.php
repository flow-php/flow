<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

/**
 * @implements NativeType<array{value: string, nullable: bool}>
 */
final class ScalarType implements NativeType
{
    public const BOOLEAN = 'boolean';

    public const FLOAT = 'float';

    public const INTEGER_32 = 'integer32';

    public const INTEGER_64 = 'integer64';

    public const STRING = 'string';

    private const INT32_MAX = 0x7FFFFFFF;

    private const INT32_MIN = -0x80000000;

    private const INT64_MAX = 0x7FFFFFFFFFFFFFFF;

    private const INT64_MIN = -0x8000000000000000;

    private function __construct(private readonly string $value, private readonly bool $nullable)
    {
    }

    public static function boolean(bool $nullable = false) : self
    {
        return new self(self::BOOLEAN, $nullable);
    }

    public static function float(bool $nullable = false) : self
    {
        return new self(self::FLOAT, $nullable);
    }

    public static function integer(bool $nullable = false) : self
    {
        if (\PHP_INT_MAX === self::INT32_MAX) {
            return self::integer32($nullable);
        }

        return self::integer64($nullable);
    }

    public static function integer32(bool $nullable = false) : self
    {
        return new self(self::INTEGER_32, $nullable);
    }

    public static function integer64(bool $nullable = false) : self
    {
        return new self(self::INTEGER_64, $nullable);
    }

    public static function string(bool $nullable = false) : self
    {
        return new self(self::STRING, $nullable);
    }

    public function __serialize() : array
    {
        return ['value' => $this->value, 'nullable' => $this->nullable];
    }

    public function __unserialize(array $data) : void
    {
        $this->value = $data['value'];
        $this->nullable = $data['nullable'];
    }

    public function isBoolean() : bool
    {
        return $this->value === self::BOOLEAN;
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $type->value === $this->value && $this->nullable === $type->nullable;
    }

    public function isFloat() : bool
    {
        return $this->value === self::FLOAT;
    }

    public function isInteger() : bool
    {
        return \in_array($this->value, [self::INTEGER_32, self::INTEGER_64], true);
    }

    public function isString() : bool
    {
        return $this->value === self::STRING;
    }

    public function isValid(mixed $value) : bool
    {
        if (null === $value && $this->nullable) {
            return true;
        }

        if (!\is_scalar($value)) {
            return false;
        }

        if ($this->value === self::FLOAT) {
            // php gettype returns double for floats for historical reasons
            if ('double' !== \gettype($value)) {
                return false;
            }
        } else {
            if ('integer' === \gettype($value)) {
                if ($this->value === self::INTEGER_32) {
                    return $value >= self::INT32_MIN && $value <= self::INT32_MAX;
                }

                if ($this->value === self::INTEGER_64) {
                    return $value >= self::INT64_MIN && $value <= self::INT64_MAX;
                }
            }

            if ($this->value !== \gettype($value)) {
                return false;
            }
        }

        return true;
    }

    public function isValidArrayKey() : bool
    {
        return $this->isString() || $this->isInteger();
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . $this->value;
    }

    public function type() : string
    {
        return $this->value;
    }
}
