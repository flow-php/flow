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

    public const INTEGER = 'integer';

    public const STRING = 'string';

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
        return new self(self::INTEGER, $nullable);
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
        return $this->value === self::INTEGER;
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
