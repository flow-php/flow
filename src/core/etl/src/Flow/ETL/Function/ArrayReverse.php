<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayReverse extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     * @param bool|ScalarFunction $preserveKeys
     */
    public function __construct(private readonly ScalarFunction|array $array, private readonly ScalarFunction|bool $preserveKeys)
    {
    }

    /**
     * @return null|array<mixed>
     */
    public function eval(Row $row) : mixed
    {
        $array = (new Parameter($this->array))->asArray($row);
        $preserveKeys = (new Parameter($this->preserveKeys))->asBoolean($row);

        if ($array === null) {
            return null;
        }

        return \array_reverse($array, $preserveKeys);
    }
}
