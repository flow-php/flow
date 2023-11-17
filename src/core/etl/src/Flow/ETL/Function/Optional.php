<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Optional implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(private readonly ScalarFunction $function)
    {
    }

    public function eval(Row $row) : mixed
    {
        try {
            return $this->function->eval($row);
        } catch (\Exception $e) {
            return null;
        }
    }
}
