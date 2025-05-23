<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use Flow\Types\Type\Type;
use UnitEnum;

/**
 * @template T of UnitEnum
 *
 * @implements Type<T>
 */
final readonly class EnumType implements Type
{
    /**
     * @param class-string<T> $class
     */
    public function __construct(public string $class)
    {
        if ($class !== \UnitEnum::class && $this->class !== \BackedEnum::class && !\enum_exists($class)) {
            throw new InvalidArgumentException("Enum {$class} not found");
        }
    }

    /**
     * @param array{class: class-string<T>} $data
     *
     * @return EnumType<T>
     */
    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('class', $data)) {
            throw new InvalidArgumentException("Missing 'class' key in enum type definition");
        }

        return new self($data['class']);
    }

    public function assert(mixed $value) : \UnitEnum
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : \UnitEnum
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            $enumClass = $this->class;

            if (\is_a($enumClass, \BackedEnum::class, true)) {
                return $enumClass::from($value);
            }

            throw new CastingException($value, $this);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        return \is_a($value, $this->class, true);
    }

    public function normalize() : array
    {
        return [
            'type' => 'enum',
            'class' => $this->class,
        ];
    }

    public function toString() : string
    {
        return 'enum<' . $this->class . '>';
    }
}
