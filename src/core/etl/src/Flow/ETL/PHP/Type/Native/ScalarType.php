<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

final class ScalarType implements NativeType
{
    public const BOOLEAN = 'boolean';

    public const FLOAT = 'float';

    public const INTEGER = 'integer';

    public const STRING = 'string';

    /**
     * @param self::* $type
     */
    private function __construct(private readonly string $type, private readonly bool $nullable)
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
        return new self(self::INTEGER, $nullable);
    }

    public static function string(bool $nullable = false) : self
    {
        return new self(self::STRING, $nullable);
    }

    public function isBoolean() : bool
    {
        return $this->type === self::BOOLEAN;
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $type->type === $this->type && $this->nullable === $type->nullable;
    }

    public function isFloat() : bool
    {
        return $this->type === self::FLOAT;
    }

    public function isInteger() : bool
    {
        return $this->type === self::INTEGER;
    }

    public function isString() : bool
    {
        return $this->type === self::STRING;
    }

    public function isValid(mixed $value) : bool
    {
        if (null === $value && $this->nullable) {
            return true;
        }

        return match ($this->type) {
            self::STRING => \is_string($value),
            self::INTEGER => \is_int($value),
            self::FLOAT => \is_float($value),
            self::BOOLEAN => \is_bool($value),
        };
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . $this->type;
    }

    public function type() : string
    {
        return $this->type;
    }

    public function makeNullable(bool $nullable): Type
    {
        return new self($this->type, $nullable);
    }
}
