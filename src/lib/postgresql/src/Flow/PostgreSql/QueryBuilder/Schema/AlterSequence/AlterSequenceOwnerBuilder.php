<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\{AlterTableCmd, AlterTableStmt, AlterTableType, DropBehavior, Node, ObjectType, RangeVar, RoleSpec, RoleSpecType};
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class AlterSequenceOwnerBuilder implements AlterSequenceOwnerFinalStep
{
    use AstToSql;

    private function __construct(
        private string $sequence,
        private ?string $schema,
        private string $owner,
        private bool $ifExists,
    ) {
    }

    public static function create(string $sequence, ?string $schema, string $owner, bool $ifExists) : self
    {
        return new self($sequence, $schema, $owner, $ifExists);
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
        $cmd->setSubtype(AlterTableType::AT_ChangeOwner);
        $cmd->setBehavior(DropBehavior::DROP_RESTRICT);

        $roleSpec = new RoleSpec();
        $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
        $roleSpec->setRolename($this->owner);

        $cmd->setNewowner($roleSpec);

        $cmdNode = new Node();
        $cmdNode->setAlterTableCmd($cmd);

        $stmt->setCmds([$cmdNode]);

        return $stmt;
    }
}
