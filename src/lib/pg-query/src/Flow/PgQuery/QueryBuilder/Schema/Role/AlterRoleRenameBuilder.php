<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Role;

use Flow\PgQuery\Protobuf\AST\{ObjectType, RenameStmt};

final readonly class AlterRoleRenameBuilder implements AlterRoleRenameFinalStep
{
    private function __construct(
        private string $name,
        private string $newName,
    ) {
    }

    public static function create(string $name, string $newName) : self
    {
        return new self($name, $newName);
    }

    public function toAst() : RenameStmt
    {
        $stmt = new RenameStmt();
        $stmt->setRenameType(ObjectType::OBJECT_ROLE);
        $stmt->setSubname($this->name);
        $stmt->setNewname($this->newName);

        return $stmt;
    }
}
