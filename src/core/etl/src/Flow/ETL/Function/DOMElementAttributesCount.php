<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\type_instance_of;
use Dom\HTMlElement;
use Flow\ETL\Row;

final class DOMElementAttributesCount extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|\DOMNode|HTMlElement $domElement)
    {
    }

    public function eval(Row $row) : ?int
    {
        $types = [
            type_instance_of(\DOMElement::class),
        ];

        if (\class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
        }

        $domElement = (new Parameter($this->domElement))->as($row, ...$types);

        if ($domElement === null) {
            return null;
        }

        if (!$domElement->hasAttributes()) {
            return 0;
        }

        return $domElement->attributes->length;
    }
}
