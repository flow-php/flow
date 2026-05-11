<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\AlterOwnerStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RoleSpec;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class AlterSchemaOwnerBuilder implements AlterSchemaOwnerFinalStep
{
    use AstToSql;

    private function __construct(
        private string $name,
        private string $owner,
    ) {}

    public static function create(string $name, string $owner): self
    {
        return new self($name, $owner);
    }

    public function toAst(): AlterOwnerStmt
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
