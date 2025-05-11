<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use Flow\ETL\PHP\Type\Type;

/**
 * @implements Type<\UnitEnum>
 */
final readonly class EnumType implements Type
{
    /**
     * @param class-string<\UnitEnum> $class
     */
    public function __construct(public string $class)
    {
        if (!\enum_exists($class)) {
            throw new InvalidArgumentException("Enum {$class} not found");
        }
    }

    /**
     * @template ClassType
     *
     * @param array{class: class-string<ClassType>} $data
     *
     * @return EnumType<ClassType>
     */
    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('class', $data)) {
            throw new InvalidArgumentException("Missing 'class' key in enum type definition");
        }

        return new self($data['class']);
    }

    /**
     * @param class-string<\UnitEnum> $class
     */
    public static function of(string $class) : self
    {
        return new self($class);
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
        if ($value instanceof $this->class) {
            return $value;
        }

        try {
            /** @var EnumType $type */
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
