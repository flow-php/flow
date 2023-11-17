<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class XPath implements ScalarFunction
{
    public function __construct(private readonly ScalarFunction $ref, private readonly string $path)
    {
    }

    public function eval(Row $row) : null|\DOMNode|array
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if ($value instanceof \DOMNode && !$value instanceof \DOMDocument) {
            $newDom = new \DOMDocument();
            $newNode = $newDom->importNode($value, true);
            $newDom->append($newNode);

            $value = $newDom;
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

        if ($result->length === 1) {
            return $result->item(0);
        }

        $nodes = [];

        foreach ($result as $node) {
            $nodes[] = $node;
        }

        return $nodes;
    }
}
