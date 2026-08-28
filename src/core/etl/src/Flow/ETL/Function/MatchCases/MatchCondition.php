<?php

declare(strict_types=1);

namespace Flow\ETL\Function\MatchCases;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ResolvesFromChildren;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;

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

    public function eval(Row $row, FlowContext $context): mixed
    {
        return (new Parameter($this->then))->eval($row, $context);
    }

    public function valid(Row $row, FlowContext $context): bool
    {
        return (new Parameter($this->condition))->asBoolean($row, $context) ?? false;
    }
}
