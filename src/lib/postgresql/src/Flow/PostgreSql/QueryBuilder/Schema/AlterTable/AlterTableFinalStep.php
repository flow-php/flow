<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterTable;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnDefinition, ColumnType};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\TableConstraint;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterTableFinalStep extends Sql
{
    public function addColumn(ColumnDefinition $column) : self;

    public function addConstraint(TableConstraint $constraint) : self;

    public function addInherit(string $table) : self;

    public function alterColumnDropDefault(string $column) : self;

    public function alterColumnDropNotNull(string $column) : self;

    public function alterColumnSetDefault(string $column, Expression $defaultExpression) : self;

    public function alterColumnSetNotNull(string $column) : self;

    public function alterColumnType(string $column, ColumnType $type) : self;

    public function disableTrigger(string $trigger) : self;

    public function disableTriggerAll() : self;

    public function disableTriggerUser() : self;

    public function dropColumn(string $column, bool $cascade = false) : self;

    public function dropColumnIfExists(string $column, bool $cascade = false) : self;

    public function dropConstraint(string $constraintName, bool $cascade = false) : self;

    public function dropConstraintIfExists(string $constraintName, bool $cascade = false) : self;

    public function dropInherit(string $table) : self;

    public function enableTrigger(string $trigger) : self;

    public function enableTriggerAll() : self;

    public function enableTriggerAlways(string $trigger) : self;

    public function enableTriggerReplica(string $trigger) : self;

    public function enableTriggerUser() : self;

    public function ifExists() : self;

    public function renameColumn(string $oldName, string $newName) : RenameTableBuilder;

    public function renameConstraint(string $oldName, string $newName) : RenameTableBuilder;

    public function renameTo(string $newName) : RenameTableBuilder;

    public function setLogged() : AlterTableLoggingFinalStep;

    public function setSchema(string $schema) : AlterTableSchemaBuilder;

    public function setTablespace(string $tablespace) : self;

    public function setUnlogged() : AlterTableLoggingFinalStep;

    public function toAst() : AlterTableStmt;

    public function toSql() : string;
}
