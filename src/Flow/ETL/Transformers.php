<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @psalm-immutable
 */
final class Transformers
{
    /**
     * @var Transformer[]
     */
    private array $transformers;

    public function __construct(Transformer ...$transformers)
    {
        $this->transformers = $transformers;
    }

    public function add(Transformer ...$transformers) : self
    {
        return new self(...$this->transformers, ...$transformers);
    }

    public function transform(Rows $rows) : Rows
    {
        return \array_reduce(
            $this->transformers,
            fn (Rows $rows, Transformer $transformer) : Rows => $transformer->transform($rows),
            $rows
        );
    }
}
