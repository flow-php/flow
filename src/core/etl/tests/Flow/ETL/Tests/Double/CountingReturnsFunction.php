<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Row;
use Flow\Types\Type;

/**
 * Counts returns() calls so a test can pin that a schema derived from the operand's declaration is
 * built once per eval(), not once per element.
 */
final class CountingReturnsFunction implements ScalarFunction
{
    use ScalarFunctionChain;

    public int $returnsCalls = 0;

    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        private readonly ScalarFunction $value,
        private readonly Type $type,
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
        return new self($children[0], $this->type);
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        return $this->value->eval($row, $context);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $this->returnsCalls++;

        return $this->type;
    }
}
