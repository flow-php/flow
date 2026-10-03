<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\Evaluation\TolerantEvaluation;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function array_key_exists;
use function Flow\Types\DSL\type_optional;

final class Optional implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $function,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->function];
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
        return type_optional($this->function->returns());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $evaluated = (new TolerantEvaluation())->evaluate($this->function, $rows, $context);
        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $results[] = array_key_exists($i, $evaluated['failed']) ? null : $evaluated['values'][$i];
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
