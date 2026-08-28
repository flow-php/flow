<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_integer;

final class Mod implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;

    public function __construct(ScalarFunction|int $left, ScalarFunction|int $right)
    {
        $this->left = $left instanceof ScalarFunction ? $left : lit($left);
        $this->right = $right instanceof ScalarFunction ? $right : lit($right);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->left, $this->right];
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
        return type_integer();
    }

    public function eval(Row $row, FlowContext $context): ?int
    {
        $leftValue = (new Parameter($this->left))->asInt($row, $context);
        $rightValue = (new Parameter($this->right))->asInt($row, $context);

        if ($leftValue === null || $rightValue === null) {
            throw new InvalidArgumentException('Mod function requires non-null values');
        }

        if ($rightValue === 0) {
            throw new InvalidArgumentException('Mod function cannot perform modulo by zero');
        }

        return (new Calculator())->modulus($leftValue, $rightValue);
    }
}
