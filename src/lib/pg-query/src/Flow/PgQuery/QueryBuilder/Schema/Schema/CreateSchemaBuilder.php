<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Schema;

use Flow\PgQuery\Protobuf\AST\{CreateSchemaStmt, RoleSpec, RoleSpecType};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class CreateSchemaBuilder implements CreateSchemaFinalStep, CreateSchemaOptionsStep
{
    use AstToSql;

    private function __construct(
        private ?string $name = null,
        private ?string $authRole = null,
        private bool $ifNotExists = false,
    ) {
    }

    public static function create(string $name) : CreateSchemaOptionsStep
    {
        return new self($name);
    }

    public function authorization(string $role) : CreateSchemaFinalStep
    {
        return new self(
            $this->name,
            $role,
            $this->ifNotExists,
        );
    }

    public function ifNotExists() : CreateSchemaOptionsStep
    {
        return new self(
            $this->name,
            $this->authRole,
            true,
        );
    }

    public function toAst() : CreateSchemaStmt
    {
        $stmt = new CreateSchemaStmt();

        if ($this->name !== null) {
            $stmt->setSchemaname($this->name);
        }

        if ($this->authRole !== null) {
            $roleSpec = new RoleSpec();
            $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
            $roleSpec->setRolename($this->authRole);
            $stmt->setAuthrole($roleSpec);
        }

        $stmt->setIfNotExists($this->ifNotExists);

        return $stmt;
    }
}
