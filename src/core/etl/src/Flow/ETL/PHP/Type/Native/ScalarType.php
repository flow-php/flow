<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;

final class ScalarType implements NativeType
{
    public const BOOLEAN = 'boolean';

    public const FLOAT = 'float';

    public const INTEGER = 'integer';

    public const STRING = 'string';

    private readonly string $value;

    private function __construct(string $value, private readonly bool $optional)
    {
        $this->value = match (\strtolower($value)) {
            'integer' => self::INTEGER,
            'float', 'double' => self::FLOAT,
            'string' => self::STRING,
            'boolean' => self::BOOLEAN,
            default => throw new InvalidArgumentException("Unsupported scalar type: {$value}")
        };
    }

    public static function boolean(bool $optional = false) : self
    {
        return new self(self::BOOLEAN, $optional);
    }

    public static function float(bool $optional = false) : self
    {
        return new self(self::FLOAT, $optional);
    }

    public static function fromString(string $value, bool $optional = false) : self
    {
        return new self($value, $optional);
    }

    public static function integer(bool $optional = false) : self
    {
        return new self(self::INTEGER, $optional);
    }

    public static function string(bool $optional = false) : self
    {
        return new self(self::STRING, $optional);
    }

    public function isBoolean() : bool
    {
        return $this->value === self::BOOLEAN;
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $type->value === $this->value;
    }

    public function isFloat() : bool
    {
        return $this->value === self::FLOAT;
    }

    public function isInteger() : bool
    {
        return $this->value === self::INTEGER;
    }

    public function isString() : bool
    {
        return $this->value === self::STRING;
    }

    public function isValid(mixed $value) : bool
    {
        if (null === $value && $this->optional) {
            return true;
        }

        if (!\is_scalar($value)) {
            return false;
        }

        if ($this->value === 'float') {
            // php gettype returns double for floats for historical reasons
            if ('double' !== \gettype($value)) {
                return false;
            }
        } else {
            if ($this->value !== \gettype($value)) {
                return false;
            }
        }

        return true;
    }

    public function optional() : bool
    {
        return $this->optional;
    }

    public function toString() : string
    {
        return ($this->optional ? '?' : '') . $this->value;
    }
}
