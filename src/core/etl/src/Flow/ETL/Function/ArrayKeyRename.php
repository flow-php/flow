<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ArrayDot\array_dot_rename;
use Flow\ETL\Row;

final class ArrayKeyRename extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string $path,
        private readonly string $newName
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!\is_array($value)) {
            return null;
        }

        return array_dot_rename($value, $this->path, $this->newName);
    }
}
