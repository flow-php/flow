<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Exception\MissingElementCastingException;
use Flow\Types\Type;
use Flow\Types\Type\ArrayKey;
use Flow\Types\Value\Json;
use Throwable;

use function array_diff;
use function array_is_list;
use function array_key_exists;
use function array_keys;
use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function implode;
use function is_array;
use function is_string;
use function json_decode;
use function str_starts_with;

use const JSON_THROW_ON_ERROR;

/**
 * @template-covariant T of array<array-key, mixed>
 *
 * @implements Type<T>
 */
final readonly class StructureType implements Type
{
    /**
     * @var list<StructureElement<value-of<T>>>
     */
    private array $elements;

    /**
     * @param list<StructureElement<value-of<T>>> $elements
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        array $elements,
        private bool $allowExtra = false,
    ) {
        if (0 === count($elements)) {
            throw new InvalidArgumentException('Structure must receive at least one element (required or optional).');
        }

        $seen = [];
        $duplicates = [];

        foreach ($elements as $element) {
            if (array_key_exists($element->name, $seen)) {
                $duplicates[$element->name] = true;
            }

            $seen[$element->name] = true;
        }

        if (count($duplicates)) {
            throw new InvalidArgumentException(
                'Structure element names must be unique: ' . implode(', ', array_keys($duplicates)),
            );
        }

        $this->elements = $elements;
    }

    /**
     * @template E
     *
     * @param array<array-key, StructureElement<E>|Type<E>> $elements insertion order is field order; a StructureElement value carries its own optional flag
     *
     * @return self<array<array-key, E>>
     */
    public static function fromElements(array $elements, bool $allowExtra = false): self
    {
        $list = [];

        foreach ($elements as $name => $type) {
            if ($type instanceof StructureElement) {
                if (ArrayKey::coerce($type->name) !== $name) {
                    throw new InvalidArgumentException(
                        'Structure element name "' . $type->name . '" does not match its key "' . $name . '"',
                    );
                }

                $list[] = $type;

                continue;
            }

            $list[] = new StructureElement($name, $type);
        }

        return new self($list, $allowExtra);
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return StructureType<array<array-key, mixed>>
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('structure_v2'),
            'fields' => type_list(type_structure([
                'name' => type_string(),
                'type' => type_map(type_string(), type_mixed()),
                'optional' => type_boolean(),
            ])),
            'allow_extra' => type_boolean(),
        ])->assert($data);

        $elements = [];

        foreach ($data['fields'] as $field) {
            $elements[] = new StructureElement(
                ArrayKey::coerce($field['name']),
                type_from_array($field['type']),
                $field['optional'],
            );
        }

        return new self($elements, $data['allow_extra']);
    }

    public function allowsExtra(): bool
    {
        return $this->allowExtra;
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
                $value = type_array()->assert(json_decode($value, true, 512, JSON_THROW_ON_ERROR));
            }

            if (!is_array($value)) {
                throw new CastingException($value, $this);
            }

            $castedStructure = [];

            foreach ($this->elements as $element) {
                if ($element->optional) {
                    if (!array_key_exists($element->name, $value)) {
                        continue;
                    }

                    if ($value[$element->name] === null && !$element->type->isValid(null)) {
                        throw new MissingElementCastingException(null, $element->type, (string) $element->name);
                    }

                    $castedStructure[$element->name] = $element->type->cast($value[$element->name]);

                    continue;
                }

                if (($value[$element->name] ?? null) === null && !$element->type->isValid(null)) {
                    throw new MissingElementCastingException(null, $element->type, (string) $element->name);
                }

                $castedStructure[$element->name] = $element->type->cast($value[$element->name] ?? null);
            }

            return $this->assert($castedStructure);
        } catch (Throwable $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    /**
     * @return null|StructureElement<value-of<T>>
     */
    public function element(int|string $name): ?StructureElement
    {
        foreach ($this->elements as $element) {
            if ($element->name === $name) {
                return $element;
            }
        }

        return null;
    }

    /**
     * @return list<StructureElement<value-of<T>>>
     */
    public function elements(): array
    {
        return $this->elements;
    }

    public function isValid(mixed $value): bool
    {
        if (!is_array($value)) {
            return false;
        }

        if ($value !== [] && array_is_list($value)) {
            return false;
        }

        foreach ($this->elements as $element) {
            if ($element->optional) {
                if (array_key_exists($element->name, $value) && !$element->type->isValid($value[$element->name])) {
                    return false;
                }

                continue;
            }

            if (!array_key_exists($element->name, $value) || !$element->type->isValid($value[$element->name])) {
                return false;
            }
        }

        if (!$this->allowExtra) {
            $knownKeys = [];

            foreach ($this->elements as $element) {
                $knownKeys[] = $element->name;
            }

            if (!empty(array_diff(array_keys($value), $knownKeys))) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return array{type: 'structure_v2', fields: list<array{name: string, type: array<string, mixed>, optional: bool}>, allow_extra: bool}
     */
    public function normalize(): array
    {
        $fields = [];

        foreach ($this->elements as $element) {
            $fields[] = [
                /* always a JSON string - an integer name would json_encode as a number and fail Rust's `name: String` */
                'name' => (string) $element->name,
                'type' => $element->type->normalize(),
                'optional' => $element->optional,
            ];
        }

        return [
            'type' => 'structure_v2',
            'fields' => $fields,
            'allow_extra' => $this->allowExtra,
        ];
    }

    public function toString(): string
    {
        $content = [];

        foreach ($this->elements as $element) {
            $content[] = $element->name . ($element->optional ? '?: ' : ': ') . $element->type->toString();
        }

        return 'structure{' . implode(', ', $content) . '}';
    }
}
