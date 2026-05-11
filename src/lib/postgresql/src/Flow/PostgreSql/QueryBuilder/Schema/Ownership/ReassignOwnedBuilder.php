<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Ownership;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ReassignOwnedStmt;
use Flow\PostgreSql\Protobuf\AST\RoleSpec;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class ReassignOwnedBuilder implements ReassignOwnedFinalStep, ReassignOwnedToStep
{
    use AstToSql;

    /**
     * @param list<string> $roles
     */
    private function __construct(
        private array $roles,
        private ?string $newRole = null,
    ) {}

    public static function create(string ...$roles): ReassignOwnedToStep
    {
        return new self(\array_values($roles));
    }

    public function to(string $newRole): ReassignOwnedFinalStep
    {
        return new self($this->roles, $newRole);
    }

    public function toAst(): ReassignOwnedStmt
    {
        $stmt = new ReassignOwnedStmt();

        $roleNodes = [];

        foreach ($this->roles as $roleName) {
            $roleSpec = new RoleSpec();
            $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
            $roleSpec->setRolename($roleName);

            $node = new Node();
            $node->setRoleSpec($roleSpec);
            $roleNodes[] = $node;
        }

        $stmt->setRoles($roleNodes);

        if ($this->newRole !== null) {
            $newRoleSpec = new RoleSpec();
            $newRoleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
            $newRoleSpec->setRolename($this->newRole);
            $stmt->setNewrole($newRoleSpec);
        }

        return $stmt;
    }
}
