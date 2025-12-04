<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use function Flow\Types\DSL\{type_class_string, type_literal, type_structure};
use Flow\Types\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @template T of \UnitEnum
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
     * @param array{type: 'enum', class: class-string<\UnitEnum>} $data
     *
     * @return EnumType<\UnitEnum>
     */
    public static function fromArray(array $data) : self
    {
        $data = type_structure([
            'type' => type_literal('enum'),
            'class' => type_class_string(),
        ])->assert($data);

        return new self($data['class']);
    }

    #[\Override]
    public function assert(mixed $value) : \UnitEnum
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value) : \UnitEnum
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            $enumClass = $this->class;

            if (\is_a($enumClass, \BackedEnum::class, true)) {
                if (!\is_int($value) && !\is_string($value)) {
                    throw new CastingException($value, $this);
                }

                return $enumClass::from($value);
            }

            throw new CastingException($value, $this);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    #[\Override]
    public function isValid(mixed $value) : bool
    {
        return (\is_object($value) || \is_string($value)) && \is_a($value, $this->class, true);
    }

    #[\Override]
    public function normalize() : array
    {
        return [
            'type' => 'enum',
            'class' => $this->class,
        ];
    }

    #[\Override]
    public function toString() : string
    {
        return 'enum<' . $this->class . '>';
    }
}
