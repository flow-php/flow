<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

final class Types implements \Countable
{
    /**
     * @var ?Type<mixed>
     */
    private readonly ?Type $first;

    /**
     * @var array<Type<mixed>>
     */
    private array $types;

    /**
     * @param Type<mixed> ...$types
     */
    public function __construct(Type ...$types)
    {
        $typesList = [];

        foreach ($types as $type) {
            $typesList[$type->toString()] = $type;
        }

        $this->types = \array_values($typesList);

        $this->first = $types[0] ?? null;
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
            if ($existingType->isEqual($type)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param Type<mixed> ...$types
     */
    public function only(Type ...$types) : self
    {
        $filteredTypes = \array_filter($this->types, function (Type $type) use ($types) : bool {
            foreach ($types as $onlyType) {
                if ($type->isEqual($onlyType)) {
                    return true;
                }
            }

            return false;
        });

        return new self(...$filteredTypes);
    }

    /**
     * @param Type<mixed> ...$types
     */
    public function without(Type ...$types) : self
    {
        $filteredTypes = \array_filter($this->types, function (Type $type) use ($types) : bool {
            foreach ($types as $withoutType) {
                if ($type->isEqual($withoutType)) {
                    return false;
                }
            }

            return true;
        });

        return new self(...$filteredTypes);
    }
}
