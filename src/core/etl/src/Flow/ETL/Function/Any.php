<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\Evaluation\Selection;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function array_key_exists;
use function array_map;
use function array_values;
use function Flow\Types\DSL\type_boolean;
use function range;

final readonly class Any implements ScalarFunction
{
    use ResolvesFromChildren;

    /**
     * @var array<ScalarFunction>
     */
    private array $functions;

    public function __construct(ScalarFunction ...$functions)
    {
        $this->functions = $functions;
    }

    public function and(ScalarFunction $scalarFunction): All
    {
        return new All(...$this->functions, ...[$scalarFunction]);
    }

    public function andNot(ScalarFunction $scalarFunction): All
    {
        return new All(...$this->functions, ...[new Not($scalarFunction)]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return array_values($this->functions);
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
        return (new Nullability())->any(type_boolean(), ...array_map(
            static fn(ScalarFunction $function): Type => $function->returns(),
            array_values($this->functions),
        ));
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $undecided = $rows->isEmpty() ? [] : range(0, $rows->count() - 1);
        $results = [];

        /** @var array<int, true> $sawNull */
        $sawNull = [];

        foreach ($this->functions as $function) {
            if ($undecided === []) {
                break;
            }

            $column = (new Selection($undecided))->evaluate($function, $rows, $context);
            $next = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $k => $value) {
                $i = $undecided[$k];

                if ($value === null) {
                    $sawNull[$i] = true;
                    $next[] = $i;

                    continue;
                }

                if ($value) {
                    $results[$i] = true;

                    continue;
                }

                $next[] = $i;
            }

            $undecided = $next;
        }

        foreach ($undecided as $i) {
            $results[$i] = array_key_exists($i, $sawNull) ? null : false;
        }

        $ordered = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $ordered[] = $results[$i];
        }

        return (new ResultColumn($context->backend()))->of($this, $ordered);
    }

    public function or(ScalarFunction $scalarFunction): self
    {
        return new self(...$this->functions, ...[$scalarFunction]);
    }

    public function orNot(ScalarFunction $scalarFunction): self
    {
        return new self(...$this->functions, ...[new Not($scalarFunction)]);
    }
}
