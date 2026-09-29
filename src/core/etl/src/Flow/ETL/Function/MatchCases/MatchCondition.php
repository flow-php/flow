<?php

declare(strict_types=1);

namespace Flow\ETL\Function\MatchCases;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\Selection;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ResolvesFromChildren;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function get_debug_type;
use function is_scalar;
use function sprintf;

final readonly class MatchCondition implements ScalarFunction
{
    use ResolvesFromChildren;

    private ScalarFunction $condition;

    private ScalarFunction $then;

    public function __construct(mixed $condition, mixed $then)
    {
        $this->condition = $condition instanceof ScalarFunction ? $condition : lit($condition);
        $this->then = $then instanceof ScalarFunction ? $then : lit($then);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->condition, $this->then];
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
        return $this->then->returns();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        return (new Parameter($this->then))->column($rows, $context);
    }

    /**
     * @param list<int> $indices ascending rows of $rows
     *
     * @return list<bool> one per index
     */
    public function valid(Rows $rows, array $indices, FlowContext $context): array
    {
        $column = (new Selection($indices))->evaluate($this->condition, $rows, $context);
        $valid = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->values() as $k => $value) {
            if ($value !== null && !is_scalar($value)) {
                throw new EvaluationException(
                    $indices[$k],
                    new InvalidArgumentException(sprintf('Expected type "boolean", got "%s".', get_debug_type($value))),
                );
            }

            $valid[] = (bool) $value;
        }

        return $valid;
    }
}
