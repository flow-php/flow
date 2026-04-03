<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\{AlterFunctionStmt, RenameStmt};
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterProcedureFinalStep extends Sql
{
    public function renameTo(string $newName) : self;

    public function reset(string $parameter) : self;

    public function resetAll() : self;

    public function securityDefiner() : self;

    public function securityInvoker() : self;

    public function set(string $parameter, string $value) : self;

    public function toAst() : AlterFunctionStmt|RenameStmt;

    public function toSql() : string;
}
