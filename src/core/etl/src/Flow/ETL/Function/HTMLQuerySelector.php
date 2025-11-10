<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\type_instance_of;
use Dom\{Element, HTMLDocument, HTMLElement};
use Flow\ETL\Exception\RequiredPHPVersionException;
use Flow\ETL\Row;

final class HTMLQuerySelector extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly ScalarFunction|string $selector,
    ) {
        if (!\class_exists('\Dom\HTMLDocument')) {
            throw new RequiredPHPVersionException('\Dom\HTMLDocument', '8.4');
        }
    }

    public function eval(Row $row) : ?Element
    {
        $value = (new Parameter($this->value))->as($row, type_instance_of(HTMLDocument::class), type_instance_of(HTMLElement::class));
        $selector = (new Parameter($this->selector))->asString($row);

        if (null === $value || null === $selector) {
            return null;
        }

        return $value->querySelector($selector);
    }
}
