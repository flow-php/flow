<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Ownership;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, DropOwnedStmt, Node, RoleSpec, RoleSpecType};

final readonly class DropOwnedBuilder implements DropOwnedFinalStep
{
    /**
     * @param list<string> $roles
     */
    private function __construct(
        private array $roles,
        private int $behavior = DropBehavior::DROP_RESTRICT,
    ) {
    }

    public static function create(string ...$roles) : DropOwnedFinalStep
    {
        return new self(\array_values($roles));
    }

    public function cascade() : DropOwnedFinalStep
    {
        return new self(
            $this->roles,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function restrict() : DropOwnedFinalStep
    {
        return new self(
            $this->roles,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function toAst() : DropOwnedStmt
    {
        $stmt = new DropOwnedStmt();
        $stmt->setBehavior($this->behavior);

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

        return $stmt;
    }
}
