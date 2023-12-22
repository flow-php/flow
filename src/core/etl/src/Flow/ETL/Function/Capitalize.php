<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Capitalize implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(private readonly ScalarFunction $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        $val = $this->ref->eval($row);

        if (!\is_string($val)) {
            return null;
        }

        if (\function_exists('mb_convert_case')) {
            return \mb_convert_case($val, \MB_CASE_TITLE);
        }

        return \ucwords($val);
    }
}
