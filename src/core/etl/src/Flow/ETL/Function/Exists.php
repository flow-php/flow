<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

final class Exists implements ScalarFunction
{
    public function __construct(private readonly ScalarFunction $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        try {
            if ($this->ref instanceof Reference) {
                return $row->has($this->ref->name());
            }

            $this->ref->eval($row);

            return true;
        } catch (\Exception $e) {
            return false;
        }
    }
}
