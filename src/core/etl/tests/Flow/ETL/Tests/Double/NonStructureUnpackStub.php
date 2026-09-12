<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Row;
use Flow\Types\Type;

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

    /**
     * @return array<array-key, mixed>
     */
    public function eval(Row $row, FlowContext $context): array
    {
        return [];
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }
}
