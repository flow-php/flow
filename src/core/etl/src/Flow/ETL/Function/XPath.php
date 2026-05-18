<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DOMDocument;
use DOMNameSpaceNode;
use DOMNode;
use DOMXPath;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class XPath extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly ScalarFunction|string $path,
    ) {}

    /**
     * @return null|array<\DOMNode>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $value = (new Parameter($this->value))->asInstanceOf($row, $context, DOMNode::class);
        $path = (new Parameter($this->path))->asString($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('XPath requires non-null DOMNode value'));
        }

        if ($path === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('XPath requires non-null path'));
        }

        if ($value instanceof DOMNode && !$value instanceof DOMDocument) {
            $dom = $value->ownerDocument ?? new DOMDocument();
            $importedNode = $dom->importNode($value, true);

            if (!$importedNode->parentNode) {
                $dom->appendChild($importedNode);
            }

            $value = $dom;
        }

        $xpath = new DOMXPath($value);
        $result = @$xpath->query($path);

        if ($result === false) {
            return null;
        }

        if ($result->length === 0) {
            return null;
        }

        $nodes = [];

        foreach ($result as $node) {
            if ($node instanceof DOMNameSpaceNode) {
                continue;
            }

            $nodes[] = $node;
        }

        return $nodes;
    }
}
