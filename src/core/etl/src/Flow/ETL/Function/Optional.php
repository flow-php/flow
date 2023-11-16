<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Optional implements ScalarFunction
{
    use \Flow\ETL\Function\EntryScalarFunction;

    public function __construct(private readonly ScalarFunction $expression)
    {
    }

    public function eval(Row $row) : mixed
    {
        try {
            return $this->expression->eval($row);
        } catch (\Exception $e) {
            return null;
        }
    }
}
