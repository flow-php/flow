<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex;

use Flow\PgQuery\Protobuf\AST\{AlterTableCmd, AlterTableStmt, AlterTableType, Node, ObjectType, RangeVar};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class AlterTablespaceIndexBuilder implements AlterTablespaceIndexFinalStep
{
    use AstToSql;

    private function __construct(
        private string $index,
        private ?string $schema,
        private string $tablespace,
        private bool $ifExists,
    ) {
    }

    public static function create(string $index, ?string $schema, string $tablespace, bool $ifExists) : AlterTablespaceIndexFinalStep
    {
        return new self($index, $schema, $tablespace, $ifExists);
    }

    public function toAst() : AlterTableStmt
    {
        $stmt = new AlterTableStmt();
        $stmt->setObjtype(ObjectType::OBJECT_INDEX);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->index);
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

        $cmdNode = new Node();
        $cmdNode->setAlterTableCmd($cmd);

        $stmt->setCmds([$cmdNode]);

        return $stmt;
    }
}
