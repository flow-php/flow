<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Nodes;

use Flow\PgQuery\Protobuf\AST\RangeVar;

final readonly class Table
{
    public function __construct(
        private RangeVar $rangeVar,
    ) {
    }

    public function alias() : ?string
    {
        $alias = $this->rangeVar->getAlias();

        if ($alias === null) {
            return null;
        }

        $aliasname = $alias->getAliasname();

        return $aliasname !== '' ? $aliasname : null;
    }

    public function name() : string
    {
        return $this->rangeVar->getRelname();
    }

    public function raw() : RangeVar
    {
        return $this->rangeVar;
    }

    public function schema() : ?string
    {
        $schemaname = $this->rangeVar->getSchemaname();

        return $schemaname !== '' ? $schemaname : null;
    }
}
