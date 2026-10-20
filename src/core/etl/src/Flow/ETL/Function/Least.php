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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $columns = array_map(static fn(ScalarFunction $value): Column => (new Parameter($value))->column(
            $rows,
            $context,
        ), $this->values);
        $order = new ValueOrder(...array_map(static fn(Column $column): Type => $column->type(), $columns));
        $arguments = array_map(static fn(Column $column): array => $column->values(), $columns);
        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            // NULL iff every argument is NULL - nulls are skipped, never compared
            $least = null;

            foreach ($arguments as $argument) {
                // @mago-ignore analysis:mixed-assignment
                $value = $argument[$i];

                if ($value === null) {
                    continue;
                }

                if ($least === null || $order->compare($value, $least) < 0) {
                    // @mago-ignore analysis:mixed-assignment
                    $least = $value;
                }
            }

            $results[] = $least;
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
