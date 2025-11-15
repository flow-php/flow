<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\type_instance_of;
use Dom\{CharacterData, HTMLElement};
use Flow\ETL\{Exception\InvalidArgumentException, FlowContext, Function\DOM\ElementSibling, Row};

final class DOMElementSibling extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DOMNode|CharacterData|HTMLElement $element,
        private readonly ElementSibling $sibling,
        private readonly bool $allowOnlyElement,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : \DOMNode|CharacterData|HTMLElement|null
    {
        $types = [
            type_instance_of(\DOMNode::class),
        ];

        if (\class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(CharacterData::class);
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

        if ($this->allowOnlyElement) {
            if (!$node instanceof \DOMElement) {
                return $context->functions()->invalidResult(new InvalidArgumentException('DOMElementSibling with option $allowOnlyElement requires DOMElement.'));
            }

            if ($node instanceof CharacterData) {
                return $context->functions()->invalidResult(new InvalidArgumentException('DOMElementSibling with option $allowOnlyElement requires HTMLElement.'));
            }

            return $this->sibling === ElementSibling::NEXT ? $node->nextElementSibling : $node->previousElementSibling;
        }

        /* @phpstan-ignore-next-line */
        return $this->sibling === ElementSibling::NEXT ? $node->nextSibling : $node->previousSibling;
    }
}
