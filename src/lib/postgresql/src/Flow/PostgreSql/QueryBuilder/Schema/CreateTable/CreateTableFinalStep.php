<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateTable;

use Flow\PostgreSql\Protobuf\AST\CreateStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\TableConstraint;

interface CreateTableFinalStep
{
    public function constraint(TableConstraint $constraint) : self;

    public function ifNotExists() : self;

    public function inherits(string ...$tables) : self;

    public function partitionByHash(string ...$columns) : self;

    public function partitionByList(string ...$columns) : self;

    public function partitionByRange(string ...$columns) : self;

    public function tablespace(string $tablespaceName) : self;

    public function temporary() : self;

    public function toAst() : CreateStmt;

    public function toSql() : string;

    public function unlogged() : self;
}
