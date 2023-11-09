<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\NullType;

final class Types implements \Countable
{
    private readonly array $types;

    private function __construct(Type ...$types)
    {
        $this->types = \array_map(
            fn (string $type) : Type => \unserialize($type),
            \array_unique(
                \array_map(
                    fn (Type $type) : string => \serialize($type),
                    \array_filter($types, fn (Type $type) : bool => !$type instanceof NullType)
                )
            )
        );
    }

    public static function create(Type ...$types) : self
    {
        if (0 === \count($types)) {
            throw new InvalidArgumentException('Type list cannot be empty');
        }

        return new self(...$types);
    }

    public function all() : array
    {
        return $this->types;
    }

    public function count() : int
    {
        return \count($this->types);
    }

    /**
     * @param callable(Type) : bool $callable
     */
    public function filter(callable $callable) : self
    {
        return new self(...\array_filter($this->types, $callable));
    }

    public function first() : Type
    {
        return $this->types[0];
    }
}
