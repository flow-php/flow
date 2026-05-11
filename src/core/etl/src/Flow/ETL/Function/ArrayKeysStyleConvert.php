<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\StyleConverter\ArrayKeyConverter;
use Flow\ETL\Row;
use Flow\ETL\String\StringStyles;

final class ArrayKeysStyleConvert extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly StringStyles $style,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->ref))->asArray($row, $context);

        if ($array === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ArrayKeysStyleConvert function requires non-null array'));
        }

        $converter = new ArrayKeyConverter(fn(string $key): string => $this->style->convert($key));

        return $converter->convert($array);
    }
}
