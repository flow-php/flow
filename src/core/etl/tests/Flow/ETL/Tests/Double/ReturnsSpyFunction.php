<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function array_fill;
use function Flow\Types\DSL\type_integer;

/**
 * Evaluates to 1 per row and counts how often its result type is derived; with $derivable false the type cannot be
 * derived at all.
 */
final class ReturnsSpyFunction implements ScalarFunction
{
    use ScalarFunctionChain;

    public int $derivations = 0;

    public function __construct(
        public readonly bool $derivable = true,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [];
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return (new ResultColumn($context->backend()))->of(
            $this,
            $rows->isEmpty() ? [] : array_fill(0, $rows->count(), 1),
        );
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $this->derivations++;

        if (!$this->derivable) {
            throw SchemaNotDerivableException::function('counting', 'no declared type');
        }

        return type_integer();
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }
}
