<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function is_countable;
use function is_string;
use function Symfony\Component\String\s;

final class Size implements ScalarFunction
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
        return type_optional(type_integer());
    }

    public function eval(Row $row, FlowContext $context): ?int
    {
        $value = (new Parameter($this->value))->eval($row, $context);

        if (is_string($value)) {
            return s($value)->length();
        }

        if (is_countable($value)) {
            return count($value);
        }

        return null;
    }
}
