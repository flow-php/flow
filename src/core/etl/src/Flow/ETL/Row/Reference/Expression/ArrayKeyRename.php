<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use function Flow\ArrayDot\array_dot_rename;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArrayKeyRename implements Expression
{
    public function __construct(
        private readonly Expression $ref,
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
