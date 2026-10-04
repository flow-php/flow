<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\Calculator\Rounding;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_float;

final class Divide implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;
    private readonly ScalarFunction $scale;
    private readonly ScalarFunction $rounding;

    public function __construct(
        ScalarFunction|int|float|string $left,
        ScalarFunction|int|float|string $right,
        ScalarFunction|int|null $scale = null,
        ScalarFunction|Rounding|null $rounding = null,
    ) {
        $this->left = $left instanceof ScalarFunction ? $left : lit($left);
        $this->right = $right instanceof ScalarFunction ? $right : lit($right);
        $this->scale = $scale instanceof ScalarFunction ? $scale : lit($scale);
        $this->rounding = $rounding instanceof ScalarFunction ? $rounding : lit($rounding);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->left, $this->right, $this->scale, $this->rounding];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2], $children[3]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_float();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $lefts = (new Parameter($this->left))->asNumbers($rows, $context);
        $rights = (new Parameter($this->right))->asNumbers($rows, $context);
        $scales = (new Parameter($this->scale))->asInts($rows, $context);
        $roundings = (new Parameter($this->rounding))->asEnums($rows, $context, Rounding::class);
        $calculator = $context->calculator();
        $results = [];
        $i = 0;

        try {
            foreach ($lefts as $i => $leftValue) {
                $rightValue = $rights[$i];

                if ($leftValue === null || $rightValue === null) {
                    throw new InvalidArgumentException('Divide function requires non-null values');
                }

                if ($rightValue === 0) {
                    throw new InvalidArgumentException('Divide function cannot divide by zero');
                }

                $results[] = $calculator->divide($leftValue, $rightValue, $scales[$i], $roundings[$i]);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
