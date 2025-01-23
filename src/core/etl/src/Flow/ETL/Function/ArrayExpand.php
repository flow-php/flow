<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Row;

final class ArrayExpand extends ScalarFunctionChain implements ExpandResults
{
    public function __construct(private readonly ScalarFunction $ref, private readonly ArrayExpand\ArrayExpand $expand)
    {
    }

    public function eval(Row $row) : ?array
    {
        $array = (new Parameter($this->ref))->asArray($row);

        if ($array === null) {
            return null;
        }

        if ($this->expand === ArrayExpand\ArrayExpand::KEYS) {
            return \array_keys($array);
        }

        if ($this->expand === ArrayExpand\ArrayExpand::BOTH) {
            return \array_map(fn ($key, $value) => [$key => $value], \array_keys($array), $array);
        }

        return $array;
    }
}
