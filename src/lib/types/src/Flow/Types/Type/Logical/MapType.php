<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Value\Json;
use Throwable;

use function array_key_exists;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function is_array;
use function is_string;
use function json_decode;
use function sprintf;
use function str_starts_with;

use const JSON_THROW_ON_ERROR;

/**
 * @template-covariant T of array<array-key, mixed>
 *
 * @implements Type<T>
 */
final readonly class MapType implements Type
{
    /**
     * @param Type<key-of<T>> $key
     * @param Type<value-of<T>> $value
     */
    public function __construct(
        private Type $key,
        private Type $value,
    ) {}

    /**
     * @param array<string, mixed> $data
     *
     * @return MapType<array<array-key, mixed>>
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('map'),
            'key' => type_map(type_string(), type_mixed()),
            'value' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        $keyType = type_from_array($data['key']);

        if (!$keyType instanceof IntegerType && !$keyType instanceof StringType) {
            throw new InvalidArgumentException(sprintf(
                'Map key type must be IntegerType or StringType, got %s',
                $keyType::class,
            ));
        }

        return new self($keyType, type_from_array($data['value']));
    }

    /**
     * @return T
     */
    public function assert(mixed $value): array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return T
     */
    public function cast(mixed $value): array
    {
        try {
            if ($value instanceof Json) {
                $value = $value->toArray();
            }

            if (is_string($value) && (str_starts_with($value, '{') || str_starts_with($value, '['))) {
                return $this->assert(json_decode($value, true, 512, JSON_THROW_ON_ERROR));
            }

            if (!is_array($value)) {
                throw new CastingException($value, $this);
            }

            $castedMap = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $key => $item) {
                $castedKey = $this->key->cast($key);

                if (array_key_exists($castedKey, $castedMap)) {
                    throw new CastingException($value, $this);
                }

                $castedMap[$castedKey] = $this->value->cast($item);
            }

            return $this->assert($castedMap);
        } catch (Throwable $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    public function isValid(mixed $value): bool
    {
        if (!is_array($value)) {
            return false;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $item) {
            // key-of<T> resolves to array-key here, so isValid()'s @assert-if-true narrows nothing.
            // The runtime check still matters: a StringType key must reject an integer key.
            // @mago-ignore analysis:redundant-type-comparison
            if (!$this->key->isValid($key)) {
                return false;
            }

            if (!$this->value->isValid($item)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return Type<key-of<T>>
     */
    public function key(): Type
    {
        return $this->key;
    }

    /**
     * @return array{type: 'map', key: array<string, mixed>, value: array<string, mixed>}
     */
    public function normalize(): array
    {
        return [
            'type' => 'map',
            'key' => $this->key->normalize(),
            'value' => $this->value->normalize(),
        ];
    }

    public function toString(): string
    {
        return 'map<' . $this->key->toString() . ', ' . $this->value->toString() . '>';
    }

    /**
     * @return Type<value-of<T>>
     */
    public function value(): Type
    {
        return $this->value;
    }
}
