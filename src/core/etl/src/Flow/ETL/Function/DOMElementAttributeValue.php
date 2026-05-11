<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\HTMLElement;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;

final class DOMElementAttributeValue extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DOMNode|HTMLElement $domElement,
        private readonly ScalarFunction|string $attribute,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $types = [
            type_instance_of(\DOMNode::class),
            type_list(type_instance_of(\DOMNode::class)),
        ];

        if (\class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
            $types[] = type_list(type_instance_of(HTMLElement::class));
        }

        $node = (new Parameter($this->domElement))->as($row, $context, ...$types);

        if ($node instanceof \DOMDocument) {
            $node = $node->documentElement;
        }

        if (\is_array($node) && \count($node)) {
            $node = \reset($node);
        }

        $attributeName = (new Parameter($this->attribute))->asString($row, $context);

        if ($node === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('DOMElementAttributeValue requires non-null DOMNode'));
        }

        if ($attributeName === null) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('DOMElementAttributeValue requires non-null attribute name'),
                );
        }

        if (!$node instanceof \DOMNode && !$node instanceof HTMLElement || !$node->hasAttributes()) {
            return null;
        }

        if (!($namedItem = $node->attributes->getNamedItem($attributeName))) {
            return null;
        }

        return $namedItem->nodeValue;
    }
}
