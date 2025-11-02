<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\{Element, HTMLDocument};
use Flow\ETL\Row;

final class HTMLQuerySelector extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly ScalarFunction|string $selector,
    ) {
    }

    public function eval(Row $row) : ?Element
    {
        if (!\class_exists('\Dom\HTMLDocument')) {
            throw new \RuntimeException('This function requires \Dom\HTMLDocument extension available in PHP 8.4+.');
        }

        $value = (new Parameter($this->value))->asInstanceOf($row, HTMLDocument::class);
        $selector = (new Parameter($this->selector))->asString($row);

        if (null === $value || null === $selector) {
            return null;
        }

        return $value->querySelector($selector);
    }
}
