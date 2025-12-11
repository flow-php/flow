<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\Protobuf\AST\ColumnRef;

final readonly class Column
{
    public function __construct(
        private ColumnRef $columnRef,
    ) {
    }

    public function name() : ?string
    {
        $fields = $this->columnRef->getFields();

        if ($fields === null || \count($fields) === 0) {
            return null;
        }

        $fieldCount = \count($fields);
        $columnField = $fields[$fieldCount - 1];

        $star = $columnField->getAStar();

        if ($star !== null) {
            return '*';
        }

        $stringNode = $columnField->getString();

        if ($stringNode !== null) {
            return $stringNode->getSval();
        }

        return null;
    }

    public function raw() : ColumnRef
    {
        return $this->columnRef;
    }

    public function table() : ?string
    {
        $fields = $this->columnRef->getFields();

        if ($fields === null || \count($fields) <= 1) {
            return null;
        }

        $tableField = $fields[0];
        $tableString = $tableField->getString();

        if ($tableString !== null) {
            return $tableString->getSval();
        }

        return null;
    }
}
