<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;

use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_optional;

final class When implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $condition;

    private readonly ScalarFunction $then;

    /**
     * Null means "no else branch" - never lit(null), so a falsy else value stays distinguishable
     * from an absent one (Spark's CaseWhen.elseValue: Option[Expression]).
     */
    private readonly ?ScalarFunction $else;

    public function __construct(mixed $condition, mixed $then, mixed $else = null)
    {
        $this->condition = $condition instanceof ScalarFunction ? $condition : lit($condition);
        $this->then = $then instanceof ScalarFunction ? $then : lit($then);
        $this->else = $else === null ? null : ($else instanceof ScalarFunction ? $else : lit($else));
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->else === null ? [$this->condition, $this->then] : [$this->condition, $this->then, $this->else];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return count($children) === 3
            ? new self($children[0], $children[1], $children[2])
            : new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $branches = [$this->then->returns()];

        if ($this->else !== null) {
            $branches[] = $this->else->returns();
        }

        $result = (new PromotingUnifier())->unifyAll(
            NullabilityRule::ANY,
            ...$branches,
        ) ?? throw InvalidTypeException::noCommonType(...$branches);

        // No ELSE means a non-matching row yields NULL, as in SQL.
        return $this->else === null ? type_optional($result) : $result;
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $condition = (new Parameter($this->condition))->asBoolean($row, $context);

        if ($condition) {
            return (new Parameter($this->then))->eval($row, $context);
        }

        if ($this->else !== null) {
            return (new Parameter($this->else))->eval($row, $context);
        }

        return null;
    }
}
