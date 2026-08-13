<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Exception\MissingElementCastingException;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use Throwable;

use function array_diff;
use function array_intersect_key;
use function array_is_list;
use function array_key_exists;
use function array_keys;
use function array_merge;
use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_from_array;
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
     * @var array<array-key, Type<value-of<T>>>
     */
    private array $elements;

    /**
     * @var array<array-key, Type<value-of<T>>>
     */
    private array $optionalElements;

    /**
     * @param array<array-key, Type<value-of<T>>> $elements
     * @param array<array-key, Type<value-of<T>>> $optionalElements
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        array $elements,
        array $optionalElements = [],
        private bool $allowExtra = false,
    ) {
        if (0 === count($elements) && 0 === count($optionalElements)) {
            throw new InvalidArgumentException('Structure must receive at least one element (required or optional).');
        }

        $duplicateKeys = array_intersect_key($elements, $optionalElements);

        if (!empty($duplicateKeys)) {
            throw new InvalidArgumentException(
                'Element keys cannot be both required and optional: ' . implode(', ', array_keys($duplicateKeys)),
            );
        }

        $this->elements = $elements;
        $this->optionalElements = $optionalElements;
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return StructureType<array<array-key, mixed>>
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('structure'),
            'elements' => type_map(type_string(), type_map(type_string(), type_mixed())),
            'optional_elements' => type_map(type_string(), type_map(type_string(), type_mixed())),
            'allow_extra' => type_boolean(),
        ])->assert($data);

        $elements = [];

        foreach ($data['elements'] as $name => $element) {
            $elements[$name] = type_from_array($element);
        }

        $optionalElements = [];

        foreach ($data['optional_elements'] as $name => $element) {
            $optionalElements[$name] = type_from_array($element);
        }

        return new self($elements, $optionalElements, $data['allow_extra']);
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

            foreach ($this->elements as $elementName => $elementType) {
                if (($value[$elementName] ?? null) === null && !$elementType->isValid(null)) {
                    throw new MissingElementCastingException(null, $elementType, (string) $elementName);
                }

                $castedStructure[$elementName] = $elementType->cast($value[$elementName] ?? null);
            }

            foreach ($this->optionalElements as $elementName => $elementType) {
                if (!array_key_exists($elementName, $value)) {
                    continue;
                }

                if ($value[$elementName] === null && !$elementType->isValid(null)) {
                    throw new MissingElementCastingException(null, $elementType, (string) $elementName);
                }

                $castedStructure[$elementName] = $elementType->cast($value[$elementName]);
            }

            return $this->assert($castedStructure);
        } catch (Throwable $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    /**
     * @return array<array-key, Type<mixed>>
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

        foreach ($this->elements as $name => $element) {
            if (!array_key_exists($name, $value) || !$element->isValid($value[$name])) {
                return false;
            }
        }

        foreach ($this->optionalElements as $name => $element) {
            if (array_key_exists($name, $value) && !$element->isValid($value[$name])) {
                return false;
            }
        }

        if (!$this->allowExtra) {
            $allKnownKeys = array_merge(array_keys($this->elements), array_keys($this->optionalElements));
            $extraKeys = array_diff(array_keys($value), $allKnownKeys);

            if (!empty($extraKeys)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return array{type: 'structure', elements: array<array-key, array<string, mixed>>, optional_elements: array<array-key, array<string, mixed>>, allow_extra: bool}
     */
    public function normalize(): array
    {
        $elements = [];

        foreach ($this->elements as $name => $element) {
            $elements[$name] = $element->normalize();
        }

        $normalized = [
            'type' => 'structure',
            'elements' => $elements,
            'optional_elements' => [],
            'allow_extra' => false,
        ];

        if (!empty($this->optionalElements)) {
            $optionalElements = [];

            foreach ($this->optionalElements as $name => $element) {
                $optionalElements[$name] = $element->normalize();
            }
            $normalized['optional_elements'] = $optionalElements;
        }

        if ($this->allowExtra) {
            $normalized['allow_extra'] = true;
        }

        return $normalized;
    }

    /**
     * @return array<array-key, Type<mixed>>
     */
    public function optionalElements(): array
    {
        return $this->optionalElements;
    }

    public function toString(): string
    {
        $content = [];

        foreach ($this->elements as $name => $element) {
            $content[] = $name . ': ' . $element->toString();
        }

        foreach ($this->optionalElements as $name => $element) {
            $content[] = $name . '?: ' . $element->toString();
        }

        return 'structure{' . implode(', ', $content) . '}';
    }
}
