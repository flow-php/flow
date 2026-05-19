<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\CharacterData;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
use DOMNode;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function class_exists;
use function Flow\Types\DSL\type_instance_of;

final class DOMElementPreviousSibling extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|DOMNode|CharacterData|HTMLElement $element,
        private readonly bool $allowOnlyElement,
    ) {}

    public function eval(Row $row, FlowContext $context): DOMNode|CharacterData|HTMLElement|null
    {
        $types = [
            type_instance_of(DOMNode::class),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(CharacterData::class);
            $types[] = type_instance_of(HTMLElement::class);
        }

        $node = (new Parameter($this->element))->as($row, $context, ...$types);

        if ($node instanceof DOMDocument) {
            $node = $node->documentElement;
        }

        if ($this->allowOnlyElement) {
            if (!$node instanceof DOMElement) {
                return $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException(
                            'DOMElementPreviousSibling with option $allowOnlyElement requires DOMElement.',
                        ),
                    );
            }

            if ($node instanceof CharacterData) {
                return $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException(
                            'DOMElementPreviousSibling with option $allowOnlyElement requires HTMLElement.',
                        ),
                    );
            }

            return $node->previousElementSibling;
        }

        /* @phpstan-ignore-next-line */
        return $node->previousSibling;
    }
}
