<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Exists implements Expression
{
    public function __construct(private readonly Expression $ref)
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
