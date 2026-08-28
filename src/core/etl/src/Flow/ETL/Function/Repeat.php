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

final class Repeat implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $times;

    public function __construct(ScalarFunction|string $value, ScalarFunction|int $times)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->times = $times instanceof ScalarFunction ? $times : lit($times);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->times];
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

    public function eval(Row $row, FlowContext $context): string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $times = (new Parameter($this->times))->asInt($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('Repeat function requires non-null value');
        }

        if ($times === null || $times <= 0) {
            throw new InvalidArgumentException('Repeat function requires non-null, positive times');
        }

        return s($value)->repeat($times)->toString();
    }
}
