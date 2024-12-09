<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Coalesce extends ScalarFunctionChain
{
    /**
     * @param array<ScalarFunction> $values
     */
    private array $values;

    public function __construct(
        ScalarFunction ...$values,
    ) {
        $this->values = $values;
    }

    public function eval(Row $row) : mixed
    {
        foreach ($this->values as $value) {
            try {
                $result = (new Parameter($value))->eval($row);
            } catch (\Exception $e) {
                continue;
            }

            if ($result !== null) {
                return $result;
            }
        }

        return null;
    }
}
