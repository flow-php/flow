<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

final class Types implements \Countable
{
    private readonly ?Type $first;

    private readonly array $types;

    public function __construct(Type ...$types)
    {
        $this->types = \array_map(
            fn (string $type) : Type => \unserialize($type),
            \array_unique(
                \array_map(fn (Type $type) : string => \serialize($type), $types)
            )
        );
        $this->first = $this->types[0] ?? null;
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
        return new self(...\array_filter($this->types, function (Type $type) use ($types) : bool {
            foreach ($types as $withoutType) {
                if ($type->isEqual($withoutType)) {
                    return false;
                }
            }

            return true;
        }));
    }
}
