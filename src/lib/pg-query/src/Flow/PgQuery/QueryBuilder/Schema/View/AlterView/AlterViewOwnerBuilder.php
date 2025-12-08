<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterView;

use Flow\PgQuery\Protobuf\AST\{AlterTableCmd, AlterTableStmt, AlterTableType, DropBehavior, Node, ObjectType, RangeVar, RoleSpec, RoleSpecType};

final readonly class AlterViewOwnerBuilder implements AlterViewOwnerFinalStep
{
    private function __construct(
        private string $view,
        private ?string $schema,
        private string $owner,
    ) {
    }

    public static function create(string $view, ?string $schema, string $owner) : self
    {
        return new self($view, $schema, $owner);
    }

    public function toAst() : AlterTableStmt
    {
        $stmt = new AlterTableStmt();
        $stmt->setObjtype(ObjectType::OBJECT_VIEW);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->view);
        $rangeVar->setInh(true);
        $rangeVar->setRelpersistence('p');

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setRelation($rangeVar);

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
