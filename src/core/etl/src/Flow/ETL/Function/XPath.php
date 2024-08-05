<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class XPath extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref, private readonly string $path)
    {
    }

    public function eval(Row $row) : \DOMNode|array|null
    {
        $value = $this->ref->eval($row);

        if ($value instanceof \DOMNode && !$value instanceof \DOMDocument) {
            $dom = new \DOMDocument();
            $importedNode = $dom->importNode($value, true);
            $dom->appendChild($importedNode);
            $value = $dom;
        }

        if (!$value instanceof \DOMDocument) {
            return null;
        }

        $xpath = new \DOMXPath($value);
        $result = @$xpath->query($this->path);

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
