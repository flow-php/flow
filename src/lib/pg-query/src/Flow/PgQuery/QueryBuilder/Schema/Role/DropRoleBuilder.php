<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Role;

use Flow\PgQuery\Protobuf\AST\{DropRoleStmt, Node, RoleSpec, RoleSpecType};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class DropRoleBuilder implements DropRoleFinalStep
{
    use AstToSql;

    /**
     * @param list<string> $names
     */
    private function __construct(
        private array $names,
        private bool $ifExists = false,
    ) {
    }

    public static function create(string ...$names) : DropRoleFinalStep
    {
        return new self(\array_values($names));
    }

    public function ifExists() : DropRoleFinalStep
    {
        return new self(
            $this->names,
            true,
        );
    }

    public function toAst() : DropRoleStmt
    {
        $stmt = new DropRoleStmt();
        $stmt->setMissingOk($this->ifExists);

        $roleNodes = [];

        foreach ($this->names as $name) {
            $roleSpec = new RoleSpec();
            $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
            $roleSpec->setRolename($name);

            $node = new Node();
            $node->setRoleSpec($roleSpec);
            $roleNodes[] = $node;
        }

        $stmt->setRoles($roleNodes);

        return $stmt;
    }
}
