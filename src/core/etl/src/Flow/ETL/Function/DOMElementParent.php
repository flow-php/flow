<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\HTMLElement;
use DOMDocument;
use DOMNode;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function class_exists;
use function Flow\Types\DSL\type_instance_of;

final class DOMElementParent extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|DOMNode|HTMLElement $element,
    ) {}

    public function eval(Row $row, FlowContext $context): DOMNode|HTMLElement|null
    {
        $types = [
            type_instance_of(DOMNode::class),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
        }

        $node = (new Parameter($this->element))->as($row, $context, ...$types);

        if ($node instanceof DOMDocument) {
            $node = $node->documentElement;
        }

        if ($node === null) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('DOMElementParent requires non-null DOMNode or HTMLElement.'),
                );
        }

        if ($node instanceof HTMLElement) {
            // @mago-ignore analysis:less-specific-return-statement
            return $node->parentElement;
        }

        return $node->parentNode;
    }
}
