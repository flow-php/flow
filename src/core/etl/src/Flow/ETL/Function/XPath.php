<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class XPath extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly ScalarFunction|string $path
    ) {
    }

    public function eval(Row $row) : \DOMNode|array|null
    {
        $value = (new Parameter($this->value))->asInstanceOf($row, \DOMNode::class);
        $path = (new Parameter($this->path))->asString($row);

        if ($value === null || $path === null) {
            return null;
        }

        if ($value instanceof \DOMNode && !$value instanceof \DOMDocument) {
            $dom = new \DOMDocument();
            $importedNode = $dom->importNode($value, true);
            $dom->appendChild($importedNode);
            $value = $dom;
        }

        $xpath = new \DOMXPath($value);
        $result = @$xpath->query($path);

        if ($result === false) {
            return null;
        }

        if ($result->length === 0) {
            return null;
        }

        $nodes = [];

        foreach ($result as $node) {
            $nodes[] = $node;
        }

        return $nodes;
    }
}
