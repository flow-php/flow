<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class AlterSchemaRenameBuilder implements AlterSchemaRenameFinalStep
{
    use AstToSql;

    private function __construct(
        private string $name,
        private string $newName,
    ) {}

    public static function create(string $name, string $newName): self
    {
        return new self($name, $newName);
    }

    public function toAst(): RenameStmt
    {
        $stmt = new RenameStmt();
        $stmt->setRenameType(ObjectType::OBJECT_SCHEMA);
        $stmt->setSubname($this->name);
        $stmt->setNewname($this->newName);

        return $stmt;
    }
}
