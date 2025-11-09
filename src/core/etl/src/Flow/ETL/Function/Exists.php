<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Row\Reference;

final class Exists extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref)
    {
    }

    public function eval(Row $row, FlowContext $context) : bool
    {
        try {
            if ($this->ref instanceof Reference) {
                return $row->has($this->ref->name());
            }

            (new Parameter($this->ref))->eval($row, $context);

            return true;
        } catch (\Exception) {
            return false;
        }
    }
}
