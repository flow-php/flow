<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ArrayDot\array_dot_rename;
use Flow\ETL\Row;

final class ArrayKeyRename extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction|string $path,
        private readonly ScalarFunction|string $newName,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->ref))->asArray($row);
        $path = (new Parameter($this->path))->asString($row);
        $newName = (new Parameter($this->newName))->asString($row);

        if ($value === null || $path === null || $newName === null) {
            return null;
        }

        return array_dot_rename($value, $path, $newName);
    }
}
