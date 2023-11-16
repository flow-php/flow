<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Exists implements ScalarFunction
{
    public function __construct(private readonly ScalarFunction $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        try {
            if ($this->ref instanceof Row\EntryReference) {
                return $row->has($this->ref->name());
            }

            $this->ref->eval($row);

            return true;
        } catch (\Exception $e) {
            return false;
        }
    }
}
