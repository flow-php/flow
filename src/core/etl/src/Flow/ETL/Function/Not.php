<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\Types\DSL\type_boolean;

final class Not implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $value,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(type_boolean(), $this->value->returns());
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $value = (new Parameter($this->value))->eval($row, $context);

        // SQL NOT NULL is NULL - the row must drop, not flip to true.
        return $value === null ? null : !$value;
    }
}
