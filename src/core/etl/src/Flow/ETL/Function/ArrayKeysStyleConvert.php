<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\Row;
use Jawira\CaseConverter\Convert;

final class ArrayKeysStyleConvert extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly StringStyles $style
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $array = (new Parameter($this->ref))->asArray($row);

        if ($array === null) {
            return null;
        }

        $converter = (new StyleConverter\ArrayKeyConverter(
            fn (string $key) : string => (string) \call_user_func([new Convert($key), 'to' . \ucfirst($this->style->value)])
        ));

        return $converter->convert($array);
    }
}
