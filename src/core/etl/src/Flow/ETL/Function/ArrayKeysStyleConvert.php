<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\Row;

final class ArrayKeysStyleConvert extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly StringStyles $style,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $array = (new Parameter($this->ref))->asArray($row);

        if ($array === null) {
            return null;
        }

        $converter = (new StyleConverter\ArrayKeyConverter(
            fn (string $key) : string => $this->style->convert($key)
        ));

        return $converter->convert($array);
    }
}
