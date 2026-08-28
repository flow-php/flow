<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;

final class Plus implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;

    public function __construct(ScalarFunction|int|float $left, ScalarFunction|int|float $right)
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
        $left = type_bare($this->left->returns());
        $right = type_bare($this->right->returns());

        if (
            !($left instanceof IntegerType || $left instanceof FloatType)
            || !($right instanceof IntegerType || $right instanceof FloatType)
        ) {
            throw InvalidTypeException::noCommonType($left, $right);
        }

        return $left instanceof IntegerType && $right instanceof IntegerType ? type_integer() : type_float();
    }

    public function eval(Row $row, FlowContext $context): int|float|null
    {
        $leftValue = (new Parameter($this->left))->asNumber($row, $context);
        $rightValue = (new Parameter($this->right))->asNumber($row, $context);

        if ($leftValue === null || $rightValue === null) {
            throw new InvalidArgumentException('Plus function requires non-null values');
        }

        return (new Calculator())->add($leftValue, $rightValue);
    }
}
