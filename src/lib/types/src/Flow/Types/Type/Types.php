<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{type_equals, type_null};
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\UnionType;

/**
 * Unique collection of types.
 *
 * @template-covariant  T
 */
final readonly class Types implements \Countable, \Stringable
{
    /**
     * @var ?Type<T>
     */
    private ?Type $first;

    /**
     * @var array<Type<T>>
     */
    private array $types;

    /**
     * @param Type<T> ...$types
     */
    public function __construct(Type ...$types)
    {
        $this->types = $types;
        $this->first = $types[0] ?? null;
    }

    public function __toString() : string
    {
        $types = [];

        foreach ($this->types as $type) {
            $types[] = $type->toString();
        }

        return \implode(',', $types);
    }

    /**
     * @return array<Type<T>>
     */
    public function all() : array
    {
        return $this->types;
    }

    public function count() : int
    {
        return \count($this->types);
    }

    /**
     * @return Types<T>
     */
    public function deduplicate() : self
    {
        $types = [];

        foreach ($this->types as $type) {
            $types[$type->toString()] = $type;
        }

        return new self(...\array_values($types));
    }

    /**
     * @return ?Type<T>
     */
    public function first() : ?Type
    {
        return $this->first;
    }

    /**
     * @param Type<mixed> $type
     */
    public function has(Type $type) : bool
    {
        foreach ($this->types as $existingType) {
            if (type_equals($existingType, $type)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param Type<mixed> ...$types
     */
    public function hasAll(Type ...$types) : bool
    {
        foreach ($types as $type) {
            if (!$this->has($type)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param Type<mixed> ...$types
     */
    public function hasAny(Type ...$types) : bool
    {
        foreach ($this->types as $existingType) {
            foreach ($types as $type) {
                if (type_equals($existingType, $type)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @param Type<mixed> ...$types
     *
     * @return Types<T>
     */
    public function only(Type ...$types) : self
    {
        $filteredTypes = \array_filter($this->types, static function (Type $type) use ($types) : bool {
            foreach ($types as $keepType) {
                if (type_equals($type, $keepType)) {
                    return true;
                }
            }

            return false;
        });

        return new self(...$filteredTypes);
    }

    /**
     * Reduce optional types to their base types.
     *
     * @return Types<mixed>
     */
    public function reduceOptionals() : self
    {
        $types = [];

        foreach ($this->types as $type) {
            if ($type instanceof OptionalType) {
                $types[] = $type->base();
            } elseif ($type instanceof UnionType && $type->isOptionalType()) {
                $types[] = $type->types()->without(type_null())->first();
            } else {
                $types[] = $type;
            }
        }

        return new self(...\array_values(\array_filter($types)));
    }

    /**
     * @param Type<mixed> ...$types
     *
     * @return Types<T>
     */
    public function without(Type ...$types) : self
    {
        $filteredTypes = \array_filter($this->types, static function (Type $type) use ($types) : bool {
            foreach ($types as $withoutType) {
                if (type_equals($type, $withoutType)) {
                    return false;
                }
            }

            return true;
        });

        return new self(...$filteredTypes);
    }
}
