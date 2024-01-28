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

    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('class', $data)) {
            throw new InvalidArgumentException("Missing 'class' key in enum type definition");
        }

        $nullable = $data['nullable'] ?? false;

        return new self($data['class'], $nullable);
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

    public function makeNullable(bool $nullable) : self
    {
        return new self($this->class, $nullable);
    }

    public function merge(Type $type) : self
    {
        if ($type instanceof NullType) {
            return $this->makeNullable(true);
        }

        if (!$type instanceof self) {
            throw new InvalidArgumentException('Cannot merge different types, ' . $this->toString() . ' and ' . $type->toString());
        }

        return new self($this->class, $this->nullable || $type->nullable());
    }

    public function normalize() : array
    {
        return [
            'type' => 'enum',
            'class' => $this->class,
            'nullable' => $this->nullable,
        ];
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
