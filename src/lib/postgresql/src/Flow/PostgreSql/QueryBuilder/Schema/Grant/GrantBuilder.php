<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\AccessPriv;
use Flow\PostgreSql\Protobuf\AST\GrantStmt;
use Flow\PostgreSql\Protobuf\AST\GrantTargetType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\RoleSpec;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

final readonly class GrantBuilder implements GrantFinalStep, GrantOnStep, GrantToStep
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
        private bool $grantOption = false,
    ) {}

    public static function create(TablePrivilege|string ...$privileges): GrantOnStep
    {
        $privs = \array_map(static fn(TablePrivilege|string $p): string => $p instanceof TablePrivilege
            ? $p->value
            : $p, $privileges);

        return new self(\array_values($privs));
    }

    public function onAllTablesInSchema(string ...$schemas): GrantToStep
    {
        return new self(
            $this->privileges,
            GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA,
            ObjectType::OBJECT_TABLE,
            \array_values($schemas),
            $this->grantees,
            $this->grantOption,
        );
    }

    public function onTable(string ...$tables): GrantToStep
    {
        return new self(
            $this->privileges,
            GrantTargetType::ACL_TARGET_OBJECT,
            ObjectType::OBJECT_TABLE,
            \array_values($tables),
            $this->grantees,
            $this->grantOption,
        );
    }

    public function to(string ...$roles): GrantFinalStep
    {
        return new self(
            $this->privileges,
            $this->targetType,
            $this->objectType,
            $this->objects,
            \array_values($roles),
            $this->grantOption,
        );
    }

    public function toAst(): GrantStmt
    {
        $stmt = new GrantStmt();
        $stmt->setIsGrant(true);
        $stmt->setTargtype($this->targetType);
        $stmt->setObjtype($this->objectType);
        $stmt->setGrantOption($this->grantOption);

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

    public function toPublic(): GrantFinalStep
    {
        return $this->to('public');
    }

    public function withGrantOption(): GrantFinalStep
    {
        return new self($this->privileges, $this->targetType, $this->objectType, $this->objects, $this->grantees, true);
    }
}
