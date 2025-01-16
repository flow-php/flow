<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final readonly class Any implements ScalarFunction
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

    public function and(ScalarFunction $scalarFunction) : All
    {
        return new All(...$this->functions, ...[$scalarFunction]);
    }

    public function andNot(ScalarFunction $scalarFunction) : All
    {
        return new All(...$this->functions, ...[new Not($scalarFunction)]);
    }

    public function eval(Row $row) : mixed
    {
        foreach ($this->functions as $ref) {
            if ($ref->eval($row)) {
                return true;
            }
        }

        return false;
    }

    public function or(ScalarFunction $scalarFunction) : self
    {
        return new self(...$this->functions, ...[$scalarFunction]);
    }

    public function orNot(ScalarFunction $scalarFunction) : self
    {
        return new self(...$this->functions, ...[new Not($scalarFunction)]);
    }
}
