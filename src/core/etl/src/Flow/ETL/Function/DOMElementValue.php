<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_instance_of, type_list};
use Flow\ETL\Row;

final class DOMElementValue extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|\DOMNode $node)
    {
    }

    public function eval(Row $row) : mixed
    {
        $node = (new Parameter($this->node))->as($row, type_instance_of(\DOMNode::class), type_list(type_instance_of(\DOMNode::class)));

        if (\is_array($node) && \count($node)) {
            $node = \reset($node);
        }

        if ($node instanceof \DOMDocument) {
            $node = $node->documentElement;
        }

        if (!$node instanceof \DOMElement) {
            return null;
        }

        return $node->nodeValue;
    }
}
