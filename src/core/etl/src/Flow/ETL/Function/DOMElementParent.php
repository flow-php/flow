<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\type_instance_of;
use Dom\HTMLElement;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class DOMElementParent extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DOMNode|HTMLElement $element,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : \DOMNode|HTMLElement|null
    {
        $types = [
            type_instance_of(\DOMNode::class),
        ];

        if (\class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
        }

        $node = (new Parameter($this->element))->as(
            $row,
            $context,
            ...$types
        );

        if ($node instanceof \DOMDocument) {
            $node = $node->documentElement;
        }

        if (\is_array($node) && \count($node)) {
            $node = \reset($node);
        }

        if ($node === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('DOMElementParent requires non-null DOMNode'));
        }

        if ($node instanceof HTMLElement) {
            return $node->parentElement;
        }

        if ($node instanceof \DOMNode) {
            return $node->parentNode;
        }

        return null;
    }
}
