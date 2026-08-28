<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function Symfony\Component\String\s;

final class StringEqualsTo implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $string;

    public function __construct(ScalarFunction|string $value, ScalarFunction|string $string)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->string];
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
        return (new Nullability())->any(type_boolean(), $this->value->returns(), $this->string->returns());
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $string = (new Parameter($this->string))->asString($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('StringEqualsTo function requires non-null value');
        }

        if ($string === null) {
            throw new InvalidArgumentException('StringEqualsTo function requires non-null string');
        }

        return s($value)->equalsTo($string);
    }
}
