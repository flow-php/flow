<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\TypeDetector;

use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;

final class Literal implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly mixed $value,
    ) {}

    /**
     * @return list<ScalarFunction>
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
        /** @var list<ScalarFunction> $children */
        return $this;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        // lit(null) is a nullable null column - a bare NullType would count as NOT NULL.
        return $this->value === null ? type_optional(type_null()) : (new TypeDetector())->detectType($this->value);
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        return $this->value;
    }
}
