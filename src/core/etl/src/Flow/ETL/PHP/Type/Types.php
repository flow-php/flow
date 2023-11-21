<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

final class Types implements \Countable
{
    private readonly ?Type $first;

    /**
     * @var string[]
     */
    private array $types;

    public function __construct(Type ...$types)
    {
        $this->types = \array_unique(
            \array_map(fn (Type $type) : string => $type->toString(), $types)
        );
        $this->first = $types[0] ?? null;
    }

    public function count() : int
    {
        return \count($this->types);
    }

    public function first() : ?Type
    {
        return $this->first;
    }

    public function without(Type ...$types) : self
    {
        $this->types = \array_filter($this->types, function (string $type) use ($types) : bool {
            foreach ($types as $withoutType) {
                if ($type === $withoutType->toString()) {
                    return false;
                }
            }

            return true;
        });

        return $this;
    }
}
