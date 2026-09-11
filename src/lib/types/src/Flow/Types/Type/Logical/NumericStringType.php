<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Stringable;

use function is_numeric;
use function is_string;

/**
 * @template T of numeric-string
 *
 * @implements Type<T>
 */
final class NumericStringType implements Type
{
    /**
     * @return numeric-string
     */
    public function assert(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return numeric-string
     */
    public function cast(mixed $value): string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (is_numeric($value)) {
            return (string) $value;
        }

        if ($value instanceof Stringable) {
            $stringValue = (string) $value;

            if (is_numeric($stringValue)) {
                return $stringValue;
            }
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return is_string($value) && is_numeric($value);
    }

    public function normalize(): array
    {
        return [
            'type' => 'numeric-string',
        ];
    }

    public function toString(): string
    {
        return 'numeric-string';
    }
}
