<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use Throwable;

use function array_is_list;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function is_array;
use function is_string;
use function json_decode;
use function str_starts_with;

use const JSON_THROW_ON_ERROR;

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
    public function __construct(
        private Type $element,
    ) {}

    /**
     * @param array<string, mixed> $data
     *
     * @return ListType<mixed>
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('list'),
            'element' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self(type_from_array($data['element']));
    }

    public function assert(mixed $value): array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if ($value instanceof Json) {
                $value = $value->toArray();
            }

            if (is_string($value) && (str_starts_with($value, '{') || str_starts_with($value, '['))) {
                return $this->assert(json_decode($value, true, 512, JSON_THROW_ON_ERROR));
            }

            if (!is_array($value)) {
                return [$this->element()->cast($value)];
            }

            $castedList = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $key => $item) {
                $castedList[$key] = $this->element()->cast($item);
            }

            return $this->assert($castedList);
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }
    }

    /**
     * @return Type<T>
     */
    public function element(): Type
    {
        return $this->element;
    }

    public function isValid(mixed $value): bool
    {
        if (!is_array($value)) {
            return false;
        }

        if ([] !== $value && !array_is_list($value)) {
            return false;
        }

        // @mago-ignore analysis:mixed-assignment
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
    public function normalize(): array
    {
        return [
            'type' => 'list',
            'element' => $this->element->normalize(),
        ];
    }

    public function toString(): string
    {
        return 'list<' . $this->element->toString() . '>';
    }
}
