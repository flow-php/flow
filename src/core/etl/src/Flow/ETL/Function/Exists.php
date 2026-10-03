<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\Evaluation\TolerantEvaluation;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function array_fill;
use function array_key_exists;
use function Flow\Types\DSL\type_boolean;

final class Exists implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $ref,
    ) {}

    /**
     * "Can this reference be reached" is the whole question - the operand must not resolve
     * against the schema, or ref('missing')->exists() would be refused by the gate.
     *
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
        return type_boolean();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        if ($this->ref instanceof Reference) {
            $exists = $rows->schema()->findDefinition($this->ref->name()) !== null;

            // @mago-ignore analysis:possibly-invalid-argument
            return (new ResultColumn($context->backend()))->of(
                $this,
                $rows->isEmpty() ? [] : array_fill(0, $rows->count(), $exists),
            );
        }

        // the operand is left unresolved by the gate (children() is empty): a reference the batch lacks stays
        // unresolved and throws, which is this function's "no"
        $failed = (new TolerantEvaluation())->evaluate(
            (new ReferenceResolver())->resolve($this->ref, $rows->schema()),
            $rows,
            $context,
        )['failed'];
        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $results[] = !array_key_exists($i, $failed);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
