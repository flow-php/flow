<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\Expression\StyleConverter\StringStyles;
use Jawira\CaseConverter\Convert;

final class ArrayKeysStyleConvert implements Expression
{
    public function __construct(
        private readonly Expression $ref,
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
            throw new RuntimeException(\get_class($this->ref) . ' is not an array, got: ' . \gettype($array));
        }

        $converter = (new StyleConverter\ArrayKeyConverter(
            fn (string $key) : string => (string) \call_user_func([new Convert($key), 'to' . \ucfirst($this->style->value)])
        ));

        return $converter->convert($array);
    }
}
