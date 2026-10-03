<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $lefts = (new Parameter($this->left))->asInts($rows, $context);
        $rights = (new Parameter($this->right))->asInts($rows, $context);
        $calculator = $context->calculator();
        $results = [];
        $i = 0;

        try {
            foreach ($lefts as $i => $leftValue) {
                $rightValue = $rights[$i];

                if ($leftValue === null || $rightValue === null) {
                    throw new InvalidArgumentException('Mod function requires non-null values');
                }

                if ($rightValue === 0) {
                    throw new InvalidArgumentException('Mod function cannot perform modulo by zero');
                }

                $results[] = $calculator->modulus($leftValue, $rightValue);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
