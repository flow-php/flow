<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_instance_of, type_list};
use Dom\HTMLElement;
use Flow\ETL\Row;

final class DOMElementValue extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|\DOMNode|HTMLElement $node)
    {
    }

    public function eval(Row $row) : mixed
    {
        $types = [
            type_instance_of(\DOMNode::class),
            type_list(type_instance_of(\DOMNode::class)),
        ];

        if (\class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
            $types[] = type_list(type_instance_of(HTMLElement::class));
        }

        $node = (new Parameter($this->node))->as(
            $row,
            ...$types
        );

        if (\is_array($node) && \count($node)) {
            $node = \reset($node);
        }

        if ($node instanceof \DOMDocument) {
            $node = $node->documentElement;
        }

        if ($node instanceof \DOMElement) {
            return $node->nodeValue;
        }

        if ($node instanceof HTMLElement) {
            return $node->textContent;
        }

        return null;
    }
}
