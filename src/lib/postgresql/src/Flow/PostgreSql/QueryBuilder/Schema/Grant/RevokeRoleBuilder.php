<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\GrantRoleStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RoleSpec;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\QueryBuilder\AstToSql;

use function array_values;

final readonly class RevokeRoleBuilder implements RevokeRoleFinalStep, RevokeRoleFromStep
{
    use AstToSql;

    /**
     * @param list<string> $revokedRoles
     * @param list<string> $fromRoles
     */
    private function __construct(
        private array $revokedRoles,
        private array $fromRoles = [],
        private int $behavior = DropBehavior::DROP_RESTRICT,
    ) {}

    public static function create(string ...$roles): RevokeRoleFromStep
    {
        return new self(array_values($roles));
    }

    public function cascade(): RevokeRoleFinalStep
    {
        return new self($this->revokedRoles, $this->fromRoles, DropBehavior::DROP_CASCADE);
    }

    public function from(string ...$roles): RevokeRoleFinalStep
    {
        return new self($this->revokedRoles, array_values($roles), $this->behavior);
    }

    public function restrict(): RevokeRoleFinalStep
    {
        return new self($this->revokedRoles, $this->fromRoles, DropBehavior::DROP_RESTRICT);
    }

    public function toAst(): GrantRoleStmt
    {
        $stmt = new GrantRoleStmt();
        $stmt->setIsGrant(false);
        $stmt->setBehavior($this->behavior);

        $grantedRoleNodes = [];

        foreach ($this->revokedRoles as $roleName) {
            $roleSpec = new RoleSpec();
            $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
            $roleSpec->setRolename($roleName);

            $node = new Node();
            $node->setRoleSpec($roleSpec);
            $grantedRoleNodes[] = $node;
        }

        $stmt->setGrantedRoles($grantedRoleNodes);

        $granteeRoleNodes = [];

        foreach ($this->fromRoles as $roleName) {
            $roleSpec = new RoleSpec();
            $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
            $roleSpec->setRolename($roleName);

            $node = new Node();
            $node->setRoleSpec($roleSpec);
            $granteeRoleNodes[] = $node;
        }

        $stmt->setGranteeRoles($granteeRoleNodes);

        return $stmt;
    }
}
