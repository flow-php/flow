<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Window;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

use function Flow\Types\DSL\type_integer;

final class RowNumber implements WindowFunction
{
    use ResolvesFromChildren;

    public function __construct(
        private readonly ?Window $window = null,
    ) {}

    public function apply(WindowContext $window): mixed
    {
        return $window->index() + 1;
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return [];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }

    public function over(Window $window): static
    {
        return new self($window);
    }

    /**
     * NOT NULL - every row of a partition has a row number (Spark RowNumberLike/RankLike nullable=false;
     * DuckDB BIGINT).
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_integer();
    }

    public function toString(): string
    {
        return 'row_number()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
