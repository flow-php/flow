<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\CharacterData;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
use DOMNode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function class_exists;
use function count;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;
use function is_array;
use function reset;

final class DOMElementValue extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|DOMNode|CharacterData|HTMLElement $node,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $types = [
            type_instance_of(DOMNode::class),
            type_list(type_instance_of(DOMNode::class)),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(CharacterData::class);
            $types[] = type_instance_of(HTMLElement::class);
            $types[] = type_list(type_instance_of(HTMLElement::class));
        }

        $node = (new Parameter($this->node))->as($row, $context, ...$types);

        if (is_array($node) && count($node)) {
            $node = reset($node);
        }

        if ($node instanceof DOMDocument) {
            $node = $node->documentElement;
        }

        if ($node instanceof DOMElement) {
            return $node->nodeValue;
        }

        if ($node instanceof CharacterData || $node instanceof HTMLElement) {
            return $node->textContent;
        }

        return null;
    }
}
