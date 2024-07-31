<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

final class Types implements \Countable
{
    private readonly ?Type $first;

    /**
     * @var array<Type>
     */
    private array $types;

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
     * @return array<Type>
     */
    public function all() : array
    {
        return $this->types;
    }

    public function count() : int
    {
        return \count($this->types);
    }

    public function first() : ?Type
    {
        return $this->first;
    }

    public function has(Type $type) : bool
    {
        foreach ($this->types as $existingType) {
            if ($existingType->isEqual($type)) {
                return true;
            }
        }

        return false;
    }

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
