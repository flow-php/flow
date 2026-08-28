<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;

use function array_map;
use function array_values;

final class Coalesce implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $values;

    public function __construct(ScalarFunction ...$values)
    {
        $this->values = array_values($values);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->values;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self(...$children);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $branches = array_map(static fn(ScalarFunction $value): Type => $value->returns(), $this->values);

        // SQL COALESCE is nullable only when every branch is - a null in one branch falls through.
        return (
            (new PromotingUnifier())->unifyAll(
                NullabilityRule::ALL,
                ...$branches,
            ) ?? throw InvalidTypeException::noCommonType(...$branches)
        );
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        foreach ($this->values as $value) {
            $result = (new Parameter($value))->eval($row, $context);

            if ($result !== null) {
                return $result;
            }
        }

        return null;
    }
}
