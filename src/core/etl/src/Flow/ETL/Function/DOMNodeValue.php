<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DOMNodeValue extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!$value instanceof \DOMNode) {
            return null;
        }

        return $value->nodeValue;
    }
}
