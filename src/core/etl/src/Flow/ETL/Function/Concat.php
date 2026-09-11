<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function array_map;
use function array_values;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function implode;

final class Concat implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $refs;

    public function __construct(ScalarFunction|string ...$refs)
    {
        $this->refs = array_values(array_map(static fn(ScalarFunction|string $ref): ScalarFunction => $ref
            instanceof ScalarFunction
                ? $ref
                : lit($ref), $refs));
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->refs;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self(...$children);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): string
    {
        /** @var array<string> $concatValues */
        $concatValues = [];

        foreach ($this->refs as $ref) {
            $value = (new Parameter($ref))->eval($row, $context);

            if ($value === null) {
                continue;
            }

            $concatValues[] = type_string()->cast($value);
        }

        return implode('', $concatValues);
    }
}
