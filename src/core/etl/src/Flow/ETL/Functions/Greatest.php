<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Kernels\Comparison\ValueOrder;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\StrictUnifier;
use SensitiveParameter;

use function array_map;
use function array_values;

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
                : new Literal($value), $values));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(array_values($rebuild->all($data['values'] ?? null, ScalarFunction::class)));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['values' => $this->values]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $columns = array_map(static fn(ScalarFunction $value): Column => $value->eval($rows, $backend), $this->values);
        $order = new ValueOrder(...array_map(static fn(Column $column): Type => $column->type(), $columns));
        $arguments = array_map(static fn(Column $column): array => $column->values(), $columns);
        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            // NULL iff every argument is NULL - nulls are skipped, never compared
            $greatest = null;

            foreach ($arguments as $argument) {
                // @mago-ignore analysis:mixed-assignment
                $value = $argument[$i];

                if ($value === null) {
                    continue;
                }

                if ($greatest === null || $order->compare($greatest, $value) < 0) {
                    // @mago-ignore analysis:mixed-assignment
                    $greatest = $value;
                }
            }

            $results[] = $greatest;
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
