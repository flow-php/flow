<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\AST\Transformers\SortOrder;
use Flow\PostgreSql\Protobuf\AST\{SortBy, SortByDir};

final readonly class OrderByItem
{
    public function __construct(
        private SortBy $sortBy,
    ) {
    }

    public function column() : ?string
    {
        $node = $this->sortBy->getNode();

        if ($node === null) {
            return null;
        }

        $columnRef = $node->getColumnRef();

        if ($columnRef === null) {
            return null;
        }

        $fields = $columnRef->getFields();

        if ($fields === null || \count($fields) === 0) {
            return null;
        }

        $fieldCount = \count($fields);
        $columnField = $fields[$fieldCount - 1];
        $stringNode = $columnField->getString();

        if ($stringNode !== null) {
            return $stringNode->getSval();
        }

        return null;
    }

    public function direction() : SortOrder
    {
        $dir = $this->sortBy->getSortbyDir();

        return match ($dir) {
            SortByDir::SORTBY_DESC => SortOrder::DESC,
            default => SortOrder::ASC,
        };
    }

    public function raw() : SortBy
    {
        return $this->sortBy;
    }
}
