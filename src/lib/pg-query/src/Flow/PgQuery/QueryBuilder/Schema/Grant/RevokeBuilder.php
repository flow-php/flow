<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Grant;

use Flow\PgQuery\Protobuf\AST\{AccessPriv, DropBehavior, GrantStmt, GrantTargetType, Node, ObjectType, PBString, RangeVar, RoleSpec, RoleSpecType};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class RevokeBuilder implements RevokeFinalStep, RevokeFromStep, RevokeOnStep
{
    use AstToSql;

    /**
     * @param list<string> $privileges
     * @param list<string> $objects
     * @param list<string> $grantees
     */
    private function __construct(
        private array $privileges,
        private int $targetType = GrantTargetType::ACL_TARGET_OBJECT,
        private int $objectType = ObjectType::OBJECT_TABLE,
        private array $objects = [],
        private array $grantees = [],
        private int $behavior = DropBehavior::DROP_RESTRICT,
    ) {
    }

    public static function create(TablePrivilege|string ...$privileges) : RevokeOnStep
    {
        $privs = \array_map(
            static fn (TablePrivilege|string $p) : string => $p instanceof TablePrivilege ? $p->value : $p,
            $privileges,
        );

        return new self(\array_values($privs));
    }

    public function cascade() : RevokeFinalStep
    {
        return new self(
            $this->privileges,
            $this->targetType,
            $this->objectType,
            $this->objects,
            $this->grantees,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function from(string ...$roles) : RevokeFinalStep
    {
        return new self(
            $this->privileges,
            $this->targetType,
            $this->objectType,
            $this->objects,
            \array_values($roles),
            $this->behavior,
        );
    }

    public function fromPublic() : RevokeFinalStep
    {
        return $this->from('public');
    }

    public function onAllTablesInSchema(string ...$schemas) : RevokeFromStep
    {
        return new self(
            $this->privileges,
            GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA,
            ObjectType::OBJECT_TABLE,
            \array_values($schemas),
            $this->grantees,
            $this->behavior,
        );
    }

    public function onTable(string ...$tables) : RevokeFromStep
    {
        return new self(
            $this->privileges,
            GrantTargetType::ACL_TARGET_OBJECT,
            ObjectType::OBJECT_TABLE,
            \array_values($tables),
            $this->grantees,
            $this->behavior,
        );
    }

    public function restrict() : RevokeFinalStep
    {
        return new self(
            $this->privileges,
            $this->targetType,
            $this->objectType,
            $this->objects,
            $this->grantees,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function toAst() : GrantStmt
    {
        $stmt = new GrantStmt();
        $stmt->setIsGrant(false);
        $stmt->setTargtype($this->targetType);
        $stmt->setObjtype($this->objectType);
        $stmt->setBehavior($this->behavior);

        $privilegeNodes = [];

        foreach ($this->privileges as $privilege) {
            if (\strtolower($privilege) === 'all') {
                $privilegeNodes = [];

                break;
            }

            $accessPriv = new AccessPriv();
            $accessPriv->setPrivName($privilege);

            $node = new Node();
            $node->setAccessPriv($accessPriv);
            $privilegeNodes[] = $node;
        }

        $stmt->setPrivileges($privilegeNodes);

        $objectNodes = [];

        if ($this->targetType === GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA) {
            foreach ($this->objects as $schemaName) {
                $str = new PBString();
                $str->setSval($schemaName);

                $node = new Node();
                $node->setString($str);
                $objectNodes[] = $node;
            }
        } else {
            foreach ($this->objects as $tableName) {
                $identifier = QualifiedIdentifier::parse($tableName);
                $rangeVar = new RangeVar();

                $schema = $identifier->schema();

                if ($schema !== null) {
                    $rangeVar->setSchemaname($schema);
                }

                $rangeVar->setRelname($identifier->name());
                $rangeVar->setInh(true);

                $node = new Node();
                $node->setRangeVar($rangeVar);
                $objectNodes[] = $node;
            }
        }

        $stmt->setObjects($objectNodes);

        $granteeNodes = [];

        foreach ($this->grantees as $grantee) {
            $roleSpec = new RoleSpec();

            if (\strtolower($grantee) === 'public') {
                $roleSpec->setRoletype(RoleSpecType::ROLESPEC_PUBLIC);
            } else {
                $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
                $roleSpec->setRolename($grantee);
            }

            $node = new Node();
            $node->setRoleSpec($roleSpec);
            $granteeNodes[] = $node;
        }

        $stmt->setGrantees($granteeNodes);

        return $stmt;
    }
}
