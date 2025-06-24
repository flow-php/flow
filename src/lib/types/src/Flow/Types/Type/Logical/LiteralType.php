<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @template T of bool|float|int|string
 *
 * @implements Type<T>
 */
final readonly class LiteralType implements Type
{
    /**
     * @param T $value
     */
    public function __construct(
        private bool|float|int|string $value,
    ) {
    }

    public function assert(mixed $value) : bool|float|int|string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : bool|float|int|string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return $value === $this->value;
    }

    public function normalize() : array
    {
        return [
            'type' => 'literal',
            'value' => (string) $this->value,
        ];
    }

    public function toString() : string
    {
        if (\is_string($this->value)) {
            return "'{$this->value}'";
        }

        if (\is_bool($this->value)) {
            return $this->value ? 'true' : 'false';
        }

        return (string) $this->value;
    }
}
