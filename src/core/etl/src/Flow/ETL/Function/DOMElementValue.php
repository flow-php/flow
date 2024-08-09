<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DOMElementValue extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|\DOMElement $domElement)
    {
    }

    public function eval(Row $row) : mixed
    {
        $domElement = (new Parameter($this->domElement))->asInstanceOf($row, \DOMElement::class);

        if (!$domElement instanceof \DOMElement) {
            return null;
        }

        return $domElement->nodeValue;
    }
}
