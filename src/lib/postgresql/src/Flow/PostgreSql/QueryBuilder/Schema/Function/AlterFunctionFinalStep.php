<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\AlterFunctionStmt;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterFunctionFinalStep extends Sql
{
    public function cost(int $cost): self;

    public function immutable(): self;

    public function parallel(ParallelSafety $safety): self;

    public function renameTo(string $newName): self;

    public function reset(string $parameter): self;

    public function resetAll(): self;

    public function rows(int $rows): self;

    public function set(string $parameter, string $value): self;

    public function stable(): self;

    public function toAst(): AlterFunctionStmt|RenameStmt;

    public function toSql(): string;

    public function volatile(): self;
}
