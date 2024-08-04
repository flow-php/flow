<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DOMElementAttributesCount extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref)
    {
    }

    public function eval(Row $row) : ?int
    {
        $value = $this->ref->eval($row);

        if ($value instanceof \DOMAttr) {
            return null;
        }

        if (!$value instanceof \DOMElement) {
            return null;
        }

        if (!$value->hasAttributes()) {
            return 0;
        }

        return $value->attributes->length;
    }
}
