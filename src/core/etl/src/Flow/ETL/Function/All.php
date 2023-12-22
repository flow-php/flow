<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class All implements ScalarFunction
{
    use EntryScalarFunction;

    /**
     * @var array<ScalarFunction>
     */
    private readonly array $refs;

    public function __construct(
        ScalarFunction ...$refs,
    ) {
        $this->refs = $refs;
    }

    public function eval(Row $row) : mixed
    {
        foreach ($this->refs as $ref) {
            if (!$ref->eval($row)) {
                return false;
            }
        }

        return true;
    }
}
