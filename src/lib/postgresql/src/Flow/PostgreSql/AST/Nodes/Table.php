<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\Protobuf\AST\RangeVar;

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

        return $alias->getAliasname() ?: null;
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
        return $this->rangeVar->getSchemaname() ?: null;
    }
}
