<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\Row;
use Jawira\CaseConverter\Convert;

final class ArrayKeysStyleConvert implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly StringStyles $style
    ) {
        if (!\class_exists(\Jawira\CaseConverter\Convert::class)) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please add jawira/case-converter dependency to the project first.");
        }
    }

    public function eval(Row $row) : mixed
    {
        $array = $this->ref->eval($row);

        if (!\is_array($array)) {
            return null;
        }

        $converter = (new StyleConverter\ArrayKeyConverter(
            fn (string $key) : string => (string) \call_user_func([new Convert($key), 'to' . \ucfirst($this->style->value)])
        ));

        return $converter->convert($array);
    }
}
