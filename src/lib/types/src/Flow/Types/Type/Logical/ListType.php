<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use Throwable;

use function array_is_list;
use function Flow\Types\DSL\type_array;
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
 * @template-covariant T of list<mixed>
 *
 * @implements Type<T>
 */
final readonly class ListType implements Type
{
    /**
     * @param Type<value-of<T>> $element
     */
    public function __construct(
        private Type $element,
    ) {}

    /**
     * @param array<string, mixed> $data
     *
     * @return ListType<list<mixed>>
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('list'),
            'element' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self(type_from_array($data['element']));
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
        // hoisted above the try: the catch below re-wraps without the reason
        if (
            !is_array($value)
            && $value !== null
            && !$value instanceof Json
            && !(is_string($value) && (str_starts_with($value, '{') || str_starts_with($value, '[')))
        ) {
            throw new CastingException($value, $this, reason: 'wrap the value first, e.g. type_list(type_string())');
        }

        try {
            if ($value instanceof Json) {
                $value = $value->toArray();
            }

            if (is_string($value) && (str_starts_with($value, '{') || str_starts_with($value, '['))) {
                $value = type_array()->assert(json_decode($value, true, 512, JSON_THROW_ON_ERROR));
            }

            if ($value === null) {
                throw new CastingException($value, $this);
            }

            if (!is_array($value)) {
                throw new CastingException($value, $this);
            }

            $castedList = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $key => $item) {
                $castedList[$key] = $this->element()->cast($item);
            }

            return $this->assert($castedList);
        } catch (Throwable $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    /**
     * @return Type<value-of<T>>
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
