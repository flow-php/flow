<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};

final readonly class All implements ScalarFunction
{
    /**
     * @var array<ScalarFunction>
     */
    private array $functions;

    public function __construct(
        ScalarFunction ...$functions,
    ) {
        $this->functions = $functions;
    }

    public function and(ScalarFunction $scalarFunction) : self
    {
        return new self(...$this->functions, ...[$scalarFunction]);
    }

    public function andNot(ScalarFunction $scalarFunction) : self
    {
        return new self(...$this->functions, ...[new Not($scalarFunction)]);
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        foreach ($this->functions as $ref) {
            if (!(new Parameter($ref))->eval($row, $context)) {
                return false;
            }
        }

        return true;
    }

    public function or(ScalarFunction $scalarFunction) : Any
    {
        return new Any(...$this->functions, ...[$scalarFunction]);
    }

    public function orNot(ScalarFunction $scalarFunction) : Any
    {
        return new Any(...$this->functions, ...[new Not($scalarFunction)]);
    }
}
