<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $results = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ((new Parameter($this->value))->values($rows, $context) as $value) {
            // SQL NOT NULL is NULL - the row must drop, not flip to true.
            $results[] = $value === null ? null : !$value;
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
