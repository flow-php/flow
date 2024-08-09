<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Any extends ScalarFunctionChain implements CompositeScalarFunction
{
    /**
     * @var array<ScalarFunction>
     */
    private readonly array $refs;

    public function __construct(
        ScalarFunction ...$values,
    ) {
        $this->refs = $values;
    }

    public function eval(Row $row) : mixed
    {
        foreach ($this->refs as $ref) {
            if ($ref->eval($row)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return array<ScalarFunction>
     */
    public function functions() : array
    {
        return $this->refs;
    }
}
