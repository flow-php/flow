<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\type_instance_of;
use DOM\{Element, HTMLDocument};
use Dom\HTMLElement;
use Flow\ETL\Exception\RequiredPHPVersionException;
use Flow\ETL\Row;

final class HTMLQuerySelectorAll extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly ScalarFunction|string $selector,
    ) {
        if (!\class_exists('\Dom\HTMLDocument')) {
            throw new RequiredPHPVersionException('\Dom\HTMLDocument', '8.4');
        }
    }

    /**
     * @return null|array<Element>
     */
    public function eval(Row $row) : ?array
    {
        $value = (new Parameter($this->value))->as($row, type_instance_of(HTMLDocument::class), type_instance_of(HTMLElement::class));
        $selector = (new Parameter($this->selector))->asString($row);

        if (null === $value || null === $selector) {
            return null;
        }

        $result = $value->querySelectorAll($selector);

        if (0 === $result->count()) {
            return null;
        }

        $nodes = [];

        foreach ($result as $node) {
            if (!$node instanceof Element) {
                continue;
            }

            $nodes[] = $node;
        }

        return $nodes;
    }
}
