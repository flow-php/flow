<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\StrictUnifier;

use function array_map;
use function array_values;
use function Flow\ETL\DSL\lit;
use function max;

final class Greatest implements ScalarFunction
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $arguments = array_map(static fn(ScalarFunction $value): array => (new Parameter($value))->values(
            $rows,
            $context,
        ), $this->values);
        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            // NULL iff every argument is NULL - nulls are skipped, never compared
            $values = [];

            foreach ($arguments as $argument) {
                if ($argument[$i] !== null) {
                    $values[] = $argument[$i];
                }
            }

            $results[] = $values === [] ? null : max($values);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
