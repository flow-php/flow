<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\Protobuf\AST\FuncCall;

final readonly class FunctionCall
{
    public function __construct(
        private FuncCall $funcCall,
    ) {
    }

    public function name() : ?string
    {
        $funcname = $this->funcCall->getFuncname();

        if ($funcname === null || \count($funcname) === 0) {
            return null;
        }

        $funcnameCount = \count($funcname);
        $nameNode = $funcname[$funcnameCount - 1];
        $stringNode = $nameNode->getString();

        if ($stringNode !== null) {
            return $stringNode->getSval();
        }

        return null;
    }

    public function raw() : FuncCall
    {
        return $this->funcCall;
    }

    public function schema() : ?string
    {
        $funcname = $this->funcCall->getFuncname();

        if ($funcname === null || \count($funcname) <= 1) {
            return null;
        }

        $schemaNode = $funcname[0];
        $schemaString = $schemaNode->getString();

        if ($schemaString !== null) {
            return $schemaString->getSval();
        }

        return null;
    }
}
