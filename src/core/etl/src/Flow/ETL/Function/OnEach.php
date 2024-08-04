<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\array_to_row;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;

final class OnEach extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction $function,
        private readonly ScalarFunction|bool $preserveKeys = true
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = $this->ref->eval($row);

        if (!\is_array($value)) {
            return null;
        }

        $preserveKeys = \is_bool($this->preserveKeys) ? $this->preserveKeys : (bool) $this->preserveKeys->eval($row);

        $output = [];

        foreach ($value as $key => $item) {
            if ($preserveKeys) {
                try {
                    $output[$key] = $this->function->eval(array_to_row(['element' => $item]));
                } catch (InvalidArgumentException $e) {
                    $output[$key] = null;
                }
            } else {
                try {
                    $output[] = $this->function->eval(array_to_row(['element' => $item]));
                } catch (InvalidArgumentException $e) {
                    $output[] = null;
                }
            }
        }

        return $output;
    }
}
