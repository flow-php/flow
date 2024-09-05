<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Row;

final class ArraySort extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction|Sort $sortFunction,
        private readonly ScalarFunction|int|null $flags,
        private readonly ScalarFunction|bool $recursive,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $array = (new Parameter($this->ref))->asArray($row);
        $flags = (new Parameter($this->flags))->asInt($row);
        $recursive = (new Parameter($this->recursive))->asBoolean($row);
        $sortFunction = (new Parameter($this->sortFunction))->asEnum($row, Sort::class);

        if ($array === null || $sortFunction === null) {
            return null;
        }

        $this->recursiveSort($array, $sortFunction->value, $flags, $recursive);

        return $array;
    }

    private function recursiveSort(array &$array, callable $function, ?int $flags, bool $recursive) : void
    {
        /** @var mixed $value */
        foreach ($array as &$value) {
            if ($recursive && \is_array($value)) {
                $this->recursiveSort($value, $function, $flags, true);
            }
        }

        if (null !== $flags) {
            $function($array, $flags);
        } else {
            $function($array);
        }
    }
}
