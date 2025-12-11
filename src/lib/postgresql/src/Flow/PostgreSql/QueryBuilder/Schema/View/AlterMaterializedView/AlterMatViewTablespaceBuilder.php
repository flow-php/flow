<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PostgreSql\Protobuf\AST\{AlterTableCmd, AlterTableStmt, AlterTableType, DropBehavior, Node, ObjectType, RangeVar};
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class AlterMatViewTablespaceBuilder implements AlterMatViewTablespaceFinalStep
{
    use AstToSql;

    private function __construct(
        private string $view,
        private ?string $schema,
        private string $tablespace,
        private bool $ifExists,
    ) {
    }

    public static function create(string $view, ?string $schema, string $tablespace, bool $ifExists) : self
    {
        return new self($view, $schema, $tablespace, $ifExists);
    }

    public function toAst() : AlterTableStmt
    {
        $stmt = new AlterTableStmt();
        $stmt->setObjtype(ObjectType::OBJECT_MATVIEW);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->view);
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
        $cmd->setSubtype(AlterTableType::AT_SetTableSpace);
        $cmd->setName($this->tablespace);
        $cmd->setBehavior(DropBehavior::DROP_RESTRICT);

        $cmdNode = new Node();
        $cmdNode->setAlterTableCmd($cmd);

        $stmt->setCmds([$cmdNode]);

        return $stmt;
    }
}
