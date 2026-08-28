<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\Types\Type;

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

    public function eval(Row $row, FlowContext $context): bool
    {
        try {
            if ($this->ref instanceof Reference) {
                return $row->has($this->ref->name());
            }

            (new Parameter($this->ref))->eval($row, $context);

            return true;
        } catch (Exception) {
            // "Can this reference be reached" is this function's whole question - a throwing
            // operand legitimately means "no". Deliberately kept after the LENIENT removal.
            return false;
        }
    }
}
