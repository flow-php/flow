<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\{type_literal, type_string, type_structure};
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

    /**
     * @param array<string, mixed> $data
     *
     * @return LiteralType<bool|float|int|string>
     */
    public static function fromArray(array $data) : self
    {
        $data = type_structure([
            'type' => type_literal('literal'),
            'value' => type_string(),
        ])->assert($data);

        return self::createFromString($data['value']);
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

    /**
     * @return LiteralType<bool|float|int|string>
     */
    private static function createFromString(string $value) : self
    {
        if ($value === 'true') {
            // @phpstan-ignore return.type
            return new self(true);
        }

        if ($value === 'false') {
            // @phpstan-ignore return.type
            return new self(false);
        }

        if (\is_numeric($value)) {
            if (\str_contains($value, '.')) {
                // @phpstan-ignore return.type
                return new self((float) $value);
            }

            // @phpstan-ignore return.type
            return new self((int) $value);
        }

        // @phpstan-ignore return.type
        return new self($value);
    }
}
