<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Prepend implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $prefix;

    public function __construct(ScalarFunction|string $value, ScalarFunction|string $prefix)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->prefix = $prefix instanceof ScalarFunction ? $prefix : lit($prefix);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->prefix];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $prefix = (new Parameter($this->prefix))->asString($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('Prepend function requires non-null value');
        }

        if ($prefix === null) {
            return $value;
        }

        return s($value)->prepend($prefix)->toString();
    }
}
