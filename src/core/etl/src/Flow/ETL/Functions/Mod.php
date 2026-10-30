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
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_integer;

final class Mod implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;

    public function __construct(ScalarFunction|int $left, ScalarFunction|int $right)
    {
        $this->left = $left instanceof ScalarFunction ? $left : new Literal($left);
        $this->right = $right instanceof ScalarFunction ? $right : new Literal($right);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['left'] ?? null, ScalarFunction::class),
            $rebuild->of($data['right'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'left' => $this->left,
            'right' => $this->right,
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
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_integer();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $lefts = (new Parameter($this->left))->asInts($rows, $backend);
        $rights = (new Parameter($this->right))->asInts($rows, $backend);
        $calculator = new Calculator();
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
