<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_boolean;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

final class Exists extends ScalarFunctionChain implements TypedScalarFunction
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
        } catch (\Exception) {
            return false;
        }
    }

    public function returns() : Type
    {
        return type_boolean();
    }
}
