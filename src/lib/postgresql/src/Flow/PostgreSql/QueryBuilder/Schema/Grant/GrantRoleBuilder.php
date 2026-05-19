<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\GrantRoleStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RoleSpec;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\QueryBuilder\AstToSql;

use function array_values;

final readonly class GrantRoleBuilder implements GrantRoleFinalStep, GrantRoleToStep
{
    use AstToSql;

    /**
     * @param list<string> $grantedRoles
     * @param list<string> $granteeRoles
     */
    private function __construct(
        private array $grantedRoles,
        private array $granteeRoles = [],
        private bool $adminOption = false,
    ) {}

    public static function create(string ...$roles): GrantRoleToStep
    {
        return new self(array_values($roles));
    }

    public function to(string ...$roles): GrantRoleFinalStep
    {
        return new self($this->grantedRoles, array_values($roles), $this->adminOption);
    }

    public function toAst(): GrantRoleStmt
    {
        $stmt = new GrantRoleStmt();
        $stmt->setIsGrant(true);

        $grantedRoleNodes = [];

        foreach ($this->grantedRoles as $roleName) {
            $roleSpec = new RoleSpec();
            $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
            $roleSpec->setRolename($roleName);

            $node = new Node();
            $node->setRoleSpec($roleSpec);
            $grantedRoleNodes[] = $node;
        }

        $stmt->setGrantedRoles($grantedRoleNodes);

        $granteeRoleNodes = [];

        foreach ($this->granteeRoles as $roleName) {
            $roleSpec = new RoleSpec();
            $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
            $roleSpec->setRolename($roleName);

            $node = new Node();
            $node->setRoleSpec($roleSpec);
            $granteeRoleNodes[] = $node;
        }

        $stmt->setGranteeRoles($granteeRoleNodes);

        if ($this->adminOption) {
            $boolean = new Boolean();
            $boolean->setBoolval(true);

            $argNode = new Node(['boolean' => $boolean]);

            $defElem = new DefElem();
            $defElem->setDefname('admin');
            $defElem->setArg($argNode);

            $node = new Node();
            $node->setDefElem($defElem);
            $stmt->setOpt([$node]);
        }

        return $stmt;
    }

    public function withAdminOption(): GrantRoleFinalStep
    {
        return new self($this->grantedRoles, $this->granteeRoles, true);
    }
}
