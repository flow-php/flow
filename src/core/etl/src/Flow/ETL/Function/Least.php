<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\StrictUnifier;

use function array_map;
use function array_values;
use function Flow\ETL\DSL\lit;
use function min;

final class Least implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $values;

    /**
     * @param array<array-key, mixed> $values
     */
    public function __construct(array $values)
    {
        $this->values = array_values(array_map(static fn(mixed $value): ScalarFunction => $value
            instanceof ScalarFunction
                ? $value
                : lit($value), $values));
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
        return new self($children);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $operands = array_map(static fn(ScalarFunction $value): Type => $value->returns(), $this->values);

        // NULL only when every argument is NULL - nulls are skipped otherwise.
        return (
            (new StrictUnifier())->unifyAll(
                NullabilityRule::ALL,
                ...$operands,
            ) ?? throw InvalidTypeException::noCommonType(...$operands)
        );
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        // NULL iff every argument is NULL - nulls are skipped, never compared
        $values = [];

        foreach ($this->values as $value) {
            $evaluated = (new Parameter($value))->eval($row, $context);

            if ($evaluated !== null) {
                $values[] = $evaluated;
            }
        }

        if ($values === []) {
            return null;
        }

        return min($values);
    }
}
