<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;

final class EnumType implements NativeType
{
    /**
     * @param class-string<\UnitEnum> $class
     */
    public function __construct(public readonly string $class, private readonly bool $nullable)
    {
        if (!\enum_exists($class)) {
            throw new InvalidArgumentException("Enum {$class} not found");
        }
    }

    /**
     * @param class-string<\UnitEnum> $class
     */
    public static function of(string $class, bool $nullable = false) : self
    {
        return new self($class, $nullable);
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $this->class === $type->class && $this->nullable === $type->nullable;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_a($value, $this->class, true);
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'enum<' . $this->class . '>';
    }
}
