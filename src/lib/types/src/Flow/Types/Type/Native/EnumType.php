<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use BackedEnum;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Throwable;
use UnitEnum;

use function enum_exists;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_structure;
use function is_a;
use function is_int;
use function is_object;
use function is_string;
use function is_subclass_of;
use function sprintf;

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
    public function __construct(
        public string $class,
    ) {
        if ($class !== UnitEnum::class && $this->class !== BackedEnum::class && !enum_exists($class)) {
            throw new InvalidArgumentException("Enum {$class} not found");
        }
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return EnumType<\UnitEnum>
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('enum'),
            'class' => type_class_string(),
        ])->assert($data);

        if (
            $data['class'] !== UnitEnum::class
            && $data['class'] !== BackedEnum::class
            && !is_subclass_of($data['class'], UnitEnum::class)
        ) {
            throw new InvalidArgumentException(sprintf('Class %s is not a UnitEnum', $data['class']));
        }

        /** @var class-string<\UnitEnum> $class */
        $class = $data['class'];

        return new self($class);
    }

    /**
     * @return T
     */
    public function assert(mixed $value): UnitEnum
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return T
     */
    public function cast(mixed $value): UnitEnum
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            $enumClass = $this->class;

            if (is_a($enumClass, BackedEnum::class, true)) {
                if (!is_int($value) && !is_string($value)) {
                    throw new CastingException($value, $this);
                }

                // is_a() above narrows $enumClass to class-string<BackedEnum>, so ::from() loses T; assert() rebinds it.
                // @mago-ignore analysis:possibly-static-access-on-interface
                return $this->assert($enumClass::from($value));
            }

            throw new CastingException($value, $this);
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value): bool
    {
        return is_object($value) && is_a($value, $this->class);
    }

    public function normalize(): array
    {
        return [
            'type' => 'enum',
            'class' => $this->class,
        ];
    }

    public function toString(): string
    {
        return 'enum<' . $this->class . '>';
    }
}
