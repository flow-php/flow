<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{type_equals, type_instance_of, type_null};
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\UnionType;

/**
 * Unique collection of types.
 */
final readonly class Types implements \Countable, \Stringable
{
    /**
     * @var ?Type<mixed>
     */
    private ?Type $first;

    /**
     * @var array<Type<mixed>>
     */
    private array $types;

    /**
     * @param Type<mixed> ...$types
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
     * @return array<Type<mixed>>
     */
    public function all() : array
    {
        return $this->types;
    }

    public function count() : int
    {
        return \count($this->types);
    }

    public function deduplicate() : self
    {
        $types = [];

        foreach ($this->types as $type) {
            $types[$type->toString()] = $type;
        }

        return new self(...\array_values($types));
    }

    /**
     * @return ?Type<mixed>
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
     */
    public function reduceOptionals() : self
    {
        $types = [];

        foreach ($this->types as $type) {
            if ($type instanceof OptionalType) {
                $types[] = $type->base();
            } elseif ($type instanceof UnionType && $type->isOptionalType()) {
                $t = type_instance_of(Type::class)->assert($type->types()->without(type_null())->first());
                $types[] = $t;
            } else {
                $types[] = $type;
            }
        }

        return new self(...$types);
    }

    /**
     * @param Type<mixed> ...$types
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
