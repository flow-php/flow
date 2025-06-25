<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\{
    type_from_array,
    type_literal,
    type_map,
    type_mixed,
    type_string,
    type_structure};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @implements Type<array<TKey, TValue>>
 */
final readonly class MapType implements Type
{
    /**
     * @param Type<TKey> $key
     * @param Type<TValue> $value
     */
    public function __construct(private Type $key, private Type $value)
    {
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return MapType<array-key, mixed>
     */
    public static function fromArray(array $data) : self
    {
        $data = type_structure([
            'type' => type_literal('map'),
            'key' => type_map(type_string(), type_mixed()),
            'value' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self(type_from_array($data['key']), type_from_array($data['value']));
    }

    public function assert(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : array
    {
        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                $decoded = \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);

                return $this->assert($decoded);
            }

            if (!\is_iterable($value)) {
                throw new CastingException($value, $this);
            }

            $castedMap = [];

            foreach ($value as $key => $item) {
                $castedKey = $this->key->cast($key);

                if (\array_key_exists($castedKey, $castedMap)) {
                    throw new CastingException($value, $this);
                }

                $castedMap[$this->key->cast($key)] = $this->value->cast($item);
            }

            return $this->assert($castedMap);
        } catch (\Throwable $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_array($value)) {
            return false;
        }

        foreach ($value as $key => $item) {
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
     * @return Type<TKey>
     */
    public function key() : Type
    {
        return $this->key;
    }

    /**
     * @return array{type: 'map', key: array<string, mixed>, value: array<string, mixed>}
     */
    public function normalize() : array
    {
        return [
            'type' => 'map',
            'key' => $this->key->normalize(),
            'value' => $this->value->normalize(),
        ];
    }

    public function toString() : string
    {
        return 'map<' . $this->key->toString() . ', ' . $this->value->toString() . '>';
    }

    /**
     * @return Type<TValue>
     */
    public function value() : Type
    {
        return $this->value;
    }
}
