<?php declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class DOMNodeValue implements Expression
{
    public function __construct(private readonly Expression $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!$value instanceof \DOMNode) {
            return null;
        }

        return $value->nodeValue;
    }
}
