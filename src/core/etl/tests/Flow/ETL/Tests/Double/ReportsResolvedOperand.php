<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function array_fill;
use function Flow\Types\DSL\type_boolean;

/**
 * Evaluates to whether its operand is resolved, so a test can pin which tree a caller evaluates.
 */
final class ReportsResolvedOperand implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $operand,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->operand];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return (new ResultColumn($context->backend()))->of($this, array_fill(
            0,
            $rows->count(),
            $this->operand->resolved(),
        ));
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_boolean();
    }
}
