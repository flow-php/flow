<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Schema;

use Flow\PgQuery\Protobuf\AST\{AlterOwnerStmt, Node, ObjectType, PBString, RoleSpec, RoleSpecType};

final readonly class AlterSchemaOwnerBuilder implements AlterSchemaOwnerFinalStep
{
    private function __construct(
        private string $name,
        private string $owner,
    ) {
    }

    public static function create(string $name, string $owner) : self
    {
        return new self($name, $owner);
    }

    public function toAst() : AlterOwnerStmt
    {
        $stmt = new AlterOwnerStmt();
        $stmt->setObjectType(ObjectType::OBJECT_SCHEMA);

        $str = new PBString();
        $str->setSval($this->name);
        $node = new Node();
        $node->setString($str);
        $stmt->setObject($node);

        $roleSpec = new RoleSpec();
        $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
        $roleSpec->setRolename($this->owner);
        $stmt->setNewowner($roleSpec);

        return $stmt;
    }
}
