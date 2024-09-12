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
        $domElement = Parameter::oneOf(
            (new Parameter($this->domElement))->asInstanceOf($row, \DOMElement::class),
            (new Parameter($this->domElement))->asInstanceOf($row, \DOMDocument::class),
            (new Parameter($this->domElement))->asListOfObjects($row, \DOMElement::class),
        );

        if (\is_array($domElement) && \count($domElement)) {
            $domElement = \reset($domElement);
        }

        if ($domElement instanceof \DOMDocument) {
            $domElement = $domElement->documentElement;
        }

        if (!$domElement instanceof \DOMElement) {
            return null;
        }

        return $domElement->nodeValue;
    }
}
