<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\AlterTable;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{AlterTableCmd, AlterTableStmt, AlterTableType, DropBehavior, Node, ObjectType, RangeVar};
use Flow\PgQuery\Protobuf\AST\ColumnDef;
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Schema\{ColumnDefinition, DataType};
use Flow\PgQuery\QueryBuilder\Schema\Constraint\TableConstraint;

final readonly class AlterTableBuilder implements AlterTableFinalStep
{
    /**
     * @param list<AlterTableCmd> $commands
     */
    private function __construct(
        private string $table,
        private ?string $schema,
        private array $commands = [],
        private bool $ifExists = false,
    ) {
    }

    public static function create(string $table, ?string $schema = null) : AlterTableFinalStep
    {
        return new self($table, $schema);
    }

    public function addColumn(ColumnDefinition $column) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_AddColumn);

        $node = new Node();
        $node->setColumnDef($column->toAst());
        $cmd->setDef($node);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function addConstraint(TableConstraint $constraint) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_AddConstraint);

        $node = new Node();
        $node->setConstraint($constraint->toAst());
        $cmd->setDef($node);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function alterColumnDropDefault(string $column) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_ColumnDefault);
        $cmd->setName($column);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function alterColumnDropNotNull(string $column) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_DropNotNull);
        $cmd->setName($column);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function alterColumnSetDefault(string $column, string $defaultExpression) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_ColumnDefault);
        $cmd->setName($column);

        $cmd->setDef($this->parseExpression($defaultExpression));

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function alterColumnSetNotNull(string $column) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_SetNotNull);
        $cmd->setName($column);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function alterColumnType(string $column, DataType $type) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_AlterColumnType);
        $cmd->setName($column);

        $columnDef = new ColumnDef();
        $columnDef->setTypeName($type->toAst());

        $node = new Node();
        $node->setColumnDef($columnDef);
        $cmd->setDef($node);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function disableTrigger(string $trigger) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_DisableTrig);
        $cmd->setName($trigger);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function disableTriggerAll() : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_DisableTrigAll);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function disableTriggerUser() : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_DisableTrigUser);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function dropColumn(string $column, bool $cascade = false) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_DropColumn);
        $cmd->setName($column);

        if ($cascade) {
            $cmd->setBehavior(DropBehavior::DROP_CASCADE);
        }

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function dropColumnIfExists(string $column, bool $cascade = false) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_DropColumn);
        $cmd->setName($column);
        $cmd->setMissingOk(true);

        if ($cascade) {
            $cmd->setBehavior(DropBehavior::DROP_CASCADE);
        }

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function dropConstraint(string $constraintName, bool $cascade = false) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_DropConstraint);
        $cmd->setName($constraintName);

        if ($cascade) {
            $cmd->setBehavior(DropBehavior::DROP_CASCADE);
        }

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function dropConstraintIfExists(string $constraintName, bool $cascade = false) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_DropConstraint);
        $cmd->setName($constraintName);
        $cmd->setMissingOk(true);

        if ($cascade) {
            $cmd->setBehavior(DropBehavior::DROP_CASCADE);
        }

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function enableTrigger(string $trigger) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_EnableTrig);
        $cmd->setName($trigger);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function enableTriggerAll() : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_EnableTrigAll);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function enableTriggerAlways(string $trigger) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_EnableAlwaysTrig);
        $cmd->setName($trigger);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function enableTriggerReplica(string $trigger) : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_EnableReplicaTrig);
        $cmd->setName($trigger);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function enableTriggerUser() : AlterTableFinalStep
    {
        $cmd = new AlterTableCmd();
        $cmd->setSubtype(AlterTableType::AT_EnableTrigUser);

        return new self(
            $this->table,
            $this->schema,
            [...$this->commands, $cmd],
            $this->ifExists,
        );
    }

    public function ifExists() : AlterTableFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->commands,
            true,
        );
    }

    public function renameColumn(string $oldName, string $newName) : RenameTableBuilder
    {
        return RenameTableBuilder::renameColumn($this->table, $this->schema, $oldName, $newName, $this->ifExists);
    }

    public function renameConstraint(string $oldName, string $newName) : RenameTableBuilder
    {
        return RenameTableBuilder::renameConstraint($this->table, $this->schema, $oldName, $newName, $this->ifExists);
    }

    public function renameTo(string $newName) : RenameTableBuilder
    {
        return RenameTableBuilder::renameTo($this->table, $this->schema, $newName, $this->ifExists);
    }

    public function setSchema(string $schema) : AlterTableSchemaBuilder
    {
        return AlterTableSchemaBuilder::create($this->table, $this->schema, $schema, $this->ifExists);
    }

    public function toAst() : AlterTableStmt
    {
        $stmt = new AlterTableStmt();

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->table);
        $rangeVar->setRelpersistence('p');
        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setRelation($rangeVar);
        $stmt->setObjtype(ObjectType::OBJECT_TABLE);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        if ($this->commands !== []) {
            $cmdNodes = [];

            foreach ($this->commands as $cmd) {
                $node = new Node();
                $node->setAlterTableCmd($cmd);
                $cmdNodes[] = $node;
            }

            $stmt->setCmds($cmdNodes);
        }

        return $stmt;
    }

    private function parseExpression(string $expression) : Node
    {
        $parser = new Parser();
        $parsed = $parser->parse("SELECT {$expression} AS x");

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $firstStmt = $stmts[0];
        $selectStmt = $firstStmt->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $targetList = $selectStmt->getTargetList();

        if ($targetList === null || \count($targetList) === 0) {
            throw InvalidAstException::invalidFieldValue('targetList', 'SelectStmt', 'expected at least one target');
        }

        $firstTarget = $targetList[0];
        $resTarget = $firstTarget->getResTarget();

        if ($resTarget === null) {
            throw InvalidAstException::unexpectedNodeType('ResTarget', 'unknown');
        }

        $val = $resTarget->getVal();

        if ($val === null) {
            throw InvalidAstException::missingRequiredField('val', 'ResTarget');
        }

        return $val;
    }
}
