<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\ArrayDot\array_dot_rename;

final class ArrayKeyRename extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction|string $path,
        private readonly ScalarFunction|string $newName,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->ref))->asArray($row, $context);
        $path = (new Parameter($this->path))->asString($row, $context);
        $newName = (new Parameter($this->newName))->asString($row, $context);

        if ($value === null || $path === null || $newName === null) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('ArrayKeyRename function requires non-null array, path, and new name'),
                );
        }

        return array_dot_rename($value, $path, $newName);
    }
}
