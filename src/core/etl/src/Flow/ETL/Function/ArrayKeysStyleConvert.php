<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Function\StyleConverter\{ArrayKeyConverter, StringStyles as OldStringStyles};
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

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $array = (new Parameter($this->ref))->asArray($row, $context);

        if ($array === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('ArrayKeysStyleConvert function requires non-null array'));
        }

        $converter = (new ArrayKeyConverter(
            fn (string $key) : string => $this->style->convert($key)
        ));

        return $converter->convert($array);
    }
}
