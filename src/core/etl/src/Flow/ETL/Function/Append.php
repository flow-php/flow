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

final class Append implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $suffix;

    public function __construct(ScalarFunction|string $value, ScalarFunction|string $suffix)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->suffix = $suffix instanceof ScalarFunction ? $suffix : lit($suffix);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->suffix];
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
        $suffix = (new Parameter($this->suffix))->asString($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('Append function requires non-null value');
        }

        if ($suffix === null) {
            return $value;
        }

        return s($value)->append($suffix)->toString();
    }
}
