<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterTable;

use Flow\PostgreSql\Protobuf\AST\{AlterTableCmd, AlterTableStmt, AlterTableType, DropBehavior, Node, ObjectType, RangeVar};
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class AlterTableLoggingBuilder implements AlterTableLoggingFinalStep
{
    use AstToSql;

    private function __construct(
        private string $table,
        private ?string $schema,
        private bool $logged,
        private bool $ifExists,
    ) {
    }

    public static function createLogged(string $table, ?string $schema, bool $ifExists) : self
    {
        return new self($table, $schema, true, $ifExists);
    }

    public static function createUnlogged(string $table, ?string $schema, bool $ifExists) : self
    {
        return new self($table, $schema, false, $ifExists);
    }

    public function toAst() : AlterTableStmt
    {
        $stmt = new AlterTableStmt();
        $stmt->setObjtype(ObjectType::OBJECT_TABLE);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->table);
        $rangeVar->setInh(true);
        $rangeVar->setRelpersistence('p');

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setRelation($rangeVar);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        $cmd = new AlterTableCmd();
        $cmd->setSubtype($this->logged ? AlterTableType::AT_SetLogged : AlterTableType::AT_SetUnLogged);
        $cmd->setBehavior(DropBehavior::DROP_RESTRICT);

        $cmdNode = new Node();
        $cmdNode->setAlterTableCmd($cmd);

        $stmt->setCmds([$cmdNode]);

        return $stmt;
    }
}
