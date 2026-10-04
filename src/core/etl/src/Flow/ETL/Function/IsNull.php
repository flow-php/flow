<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function array_fill;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;

final class IsNull implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(mixed $value)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
    }

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
        return type_boolean();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $column = (new Parameter($this->value))->column($rows, $context);

        if ($column->nullCount() === 0) {
            // @mago-ignore analysis:possibly-invalid-argument
            return (new ResultColumn($context->backend()))->of(
                $this,
                $rows->isEmpty() ? [] : array_fill(0, $rows->count(), false),
            );
        }

        $results = [];

        for ($i = 0, $count = $column->count(); $i < $count; $i++) {
            $results[] = $column->isNull($i);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
