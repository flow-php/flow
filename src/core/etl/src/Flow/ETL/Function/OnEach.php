<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\array_to_row;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;

final class OnEach extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|array $array,
        private readonly ScalarFunction $function,
        private readonly ScalarFunction|bool $preserveKeys = true,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->array))->asArray($row);
        $preserveKeys = (new Parameter($this->preserveKeys))->asBoolean($row);

        if ($value === null) {
            return null;
        }

        $output = [];

        foreach ($value as $key => $item) {
            if ($preserveKeys) {
                try {
                    $output[$key] = $this->function->eval(array_to_row(['element' => $item]));
                } catch (InvalidArgumentException) {
                    $output[$key] = null;
                }
            } else {
                try {
                    $output[] = $this->function->eval(array_to_row(['element' => $item]));
                } catch (InvalidArgumentException) {
                    $output[] = null;
                }
            }
        }

        return $output;
    }
}
