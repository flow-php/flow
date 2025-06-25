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
use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

/**
 * @template T
 *
 * @implements Type<list<T>>
 */
final readonly class ListType implements Type
{
    /**
     * @param Type<T> $element
     */
    public function __construct(private Type $element)
    {
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return ListType<mixed>
     */
    public static function fromArray(array $data) : self
    {
        $data = type_structure([
            'type' => type_literal('list'),
            'element' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self(type_from_array($data['element']));
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
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return $this->assert(\json_decode($value, true, 512, \JSON_THROW_ON_ERROR));
            }

            if (!\is_array($value)) {
                return [$this->element()->cast($value)];
            }

            $castedList = [];

            foreach ($value as $key => $item) {
                $castedList[$key] = $this->element()->cast($item);
            }

            return $this->assert($castedList);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    /**
     * @return Type<T>
     */
    public function element() : Type
    {
        return $this->element;
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_array($value)) {

            return false;
        }

        if ([] !== $value && !\array_is_list($value)) {
            return false;
        }

        foreach ($value as $item) {
            if (!$this->element->isValid($item)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return array{type: 'list', element: array<string, mixed>}
     */
    public function normalize() : array
    {
        return [
            'type' => 'list',
            'element' => $this->element->normalize(),
        ];
    }

    public function toString() : string
    {
        return 'list<' . $this->element->toString() . '>';
    }
}
