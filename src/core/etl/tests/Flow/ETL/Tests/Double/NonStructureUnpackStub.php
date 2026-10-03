<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function array_fill;
use function Flow\Types\DSL\type_string;

/**
 * An UnpackResults whose returns() breaks the structure contract, so a test can reach the guard for it.
 */
final class NonStructureUnpackStub implements UnpackResults
{
    use ScalarFunctionChain;

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
        return $this;
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return new ValueColumn(array_fill(0, $rows->count(), []));
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }
}
