<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use SensitiveParameter;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function is_float;
use function is_int;

final class Plus implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;

    public function __construct(
        ScalarFunction|int|float $left,
        ScalarFunction|int|float $right,
        private readonly bool $exact = false,
    ) {
        $this->left = $left instanceof ScalarFunction ? $left : new Literal($left);
        $this->right = $right instanceof ScalarFunction ? $right : new Literal($right);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['left'] ?? null, ScalarFunction::class),
            $rebuild->of($data['right'] ?? null, ScalarFunction::class),
            type_boolean()->assert($data['exact'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'left' => $this->left,
            'right' => $this->right,
            'exact' => $this->exact,
        ]);
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
        return new self($children[0], $children[1], $this->exact);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $lefts = (new Parameter($this->left))->asNumbers($rows, $backend);
        $rights = (new Parameter($this->right))->asNumbers($rows, $backend);
        $calculator = new Calculator();
        $results = [];
        $i = 0;

        try {
            foreach ($lefts as $i => $left) {
                $right = $rights[$i];

                if ($left === null || $right === null) {
                    throw new InvalidArgumentException('Plus function requires non-null values');
                }

                if ($this->exact) {
                    $results[] = $calculator->add($left, $right);

                    continue;
                }

                $result = $left + $right;

                if (is_int($left) && is_int($right) && is_float($result)) {
                    throw new InvalidArgumentException('Plus function integer overflow');
                }

                $results[] = $result;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
