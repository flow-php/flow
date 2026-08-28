<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function array_map;
use function array_values;
use function Flow\Types\DSL\type_boolean;

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

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $sawNull = false;

        foreach ($this->functions as $ref) {
            $value = (new Parameter($ref))->eval($row, $context);

            if ($value === null) {
                $sawNull = true;

                continue;
            }

            if ($value) {
                return true;
            }
        }

        return $sawNull ? null : false;
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
