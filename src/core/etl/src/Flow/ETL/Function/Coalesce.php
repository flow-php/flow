<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\Evaluation\Selection;
use Flow\ETL\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;

use function array_map;
use function array_values;
use function range;

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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $pending = $rows->isEmpty() ? [] : range(0, $rows->count() - 1);
        $values = [];

        foreach ($this->values as $value) {
            if ($pending === []) {
                break;
            }

            $column = (new Selection($pending))->evaluate($value, $rows, $context);
            $stillNull = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $k => $result) {
                if ($result === null) {
                    $stillNull[] = $pending[$k];

                    continue;
                }

                $values[$pending[$k]] = $result;
            }

            $pending = $stillNull;
        }

        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $results[] = $values[$i] ?? null;
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
