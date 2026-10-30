<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Calculator\Rounding;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

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
        $this->left = $left instanceof ScalarFunction ? $left : new Literal($left);
        $this->right = $right instanceof ScalarFunction ? $right : new Literal($right);
        $this->scale = $scale instanceof ScalarFunction ? $scale : new Literal($scale);
        $this->rounding = $rounding instanceof ScalarFunction ? $rounding : new Literal($rounding);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['left'] ?? null, ScalarFunction::class),
            $rebuild->of($data['right'] ?? null, ScalarFunction::class),
            $rebuild->of($data['scale'] ?? null, ScalarFunction::class),
            $rebuild->of($data['rounding'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'left' => $this->left,
            'right' => $this->right,
            'scale' => $this->scale,
            'rounding' => $this->rounding,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $lefts = (new Parameter($this->left))->asNumbers($rows, $backend);
        $rights = (new Parameter($this->right))->asNumbers($rows, $backend);
        $scales = (new Parameter($this->scale))->asInts($rows, $backend);
        $roundings = (new Parameter($this->rounding))->asEnums($rows, $backend, Rounding::class);
        $calculator = new Calculator();
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
