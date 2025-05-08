<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\StyleConverter\StringStyles as OldStringStyles;
use Flow\ETL\Row;
use Flow\ETL\String\StringStyles;

final class ArrayKeysStyleConvert extends ScalarFunctionChain
{
    private StringStyles $style;

    public function __construct(
        private readonly ScalarFunction $ref,
        OldStringStyles|StringStyles $style,
    ) {
        if ($style instanceof OldStringStyles) {
            $this->style = StringStyles::fromString($style->value);
        } else {
            $this->style = $style;
        }
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
