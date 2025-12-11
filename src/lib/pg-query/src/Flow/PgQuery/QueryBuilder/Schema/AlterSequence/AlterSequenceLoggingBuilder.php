<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\AlterSequence;

use Flow\PgQuery\Protobuf\AST\{AlterTableCmd, AlterTableStmt, AlterTableType, DropBehavior, Node, ObjectType, RangeVar};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class AlterSequenceLoggingBuilder implements AlterSequenceLoggingFinalStep
{
    use AstToSql;

    private function __construct(
        private string $sequence,
        private ?string $schema,
        private bool $logged,
        private bool $ifExists,
    ) {
    }

    public static function createLogged(string $sequence, ?string $schema, bool $ifExists) : self
    {
        return new self($sequence, $schema, true, $ifExists);
    }

    public static function createUnlogged(string $sequence, ?string $schema, bool $ifExists) : self
    {
        return new self($sequence, $schema, false, $ifExists);
    }

    public function toAst() : AlterTableStmt
    {
        $stmt = new AlterTableStmt();
        $stmt->setObjtype(ObjectType::OBJECT_SEQUENCE);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->sequence);
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
