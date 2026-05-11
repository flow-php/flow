<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

use Flow\PostgreSql\Protobuf\AST\AlterRoleStmt;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RoleSpec;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class AlterRoleBuilder implements AlterRoleActionStep, AlterRoleFinalStep
{
    use AstToSql;

    /**
     * @param list<array{name: string, arg: ?Node}> $options
     */
    private function __construct(
        private string $name,
        private array $options = [],
    ) {}

    public static function create(string $name): AlterRoleActionStep
    {
        return new self($name);
    }

    public function bypassRls(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::BYPASSRLS->value, true);
    }

    public function connectionLimit(int $limit): AlterRoleFinalStep
    {
        return $this->withIntegerOption('connectionlimit', $limit);
    }

    public function createDb(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::CREATEDB->value, true);
    }

    public function createRole(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::CREATEROLE->value, true);
    }

    public function inherit(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::INHERIT->value, true);
    }

    public function login(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::LOGIN->value, true);
    }

    public function noBypassRls(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::BYPASSRLS->value, false);
    }

    public function noCreateDb(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::CREATEDB->value, false);
    }

    public function noCreateRole(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::CREATEROLE->value, false);
    }

    public function noInherit(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::INHERIT->value, false);
    }

    public function noLogin(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::LOGIN->value, false);
    }

    public function noReplication(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::REPLICATION->value, false);
    }

    public function noSuperuser(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::SUPERUSER->value, false);
    }

    public function renameTo(string $newName): AlterRoleRenameFinalStep
    {
        return AlterRoleRenameBuilder::create($this->name, $newName);
    }

    public function replication(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::REPLICATION->value, true);
    }

    public function set(): AlterRoleFinalStep
    {
        return $this;
    }

    public function superuser(): AlterRoleFinalStep
    {
        return $this->withBooleanOption(RoleOption::SUPERUSER->value, true);
    }

    public function toAst(): AlterRoleStmt
    {
        $stmt = new AlterRoleStmt();

        $roleSpec = new RoleSpec();
        $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
        $roleSpec->setRolename($this->name);
        $stmt->setRole($roleSpec);

        $stmt->setAction(1);

        $optionNodes = [];

        foreach ($this->options as $option) {
            $defElem = new DefElem();
            $defElem->setDefname($option['name']);

            if ($option['arg'] !== null) {
                $defElem->setArg($option['arg']);
            }

            $node = new Node();
            $node->setDefElem($defElem);
            $optionNodes[] = $node;
        }

        if ($optionNodes !== []) {
            $stmt->setOptions($optionNodes);
        }

        return $stmt;
    }

    public function validUntil(string $timestamp): AlterRoleFinalStep
    {
        return $this->withStringOption('validUntil', $timestamp);
    }

    public function withPassword(#[\SensitiveParameter] string $password): AlterRoleFinalStep
    {
        return $this->withStringOption('password', $password);
    }

    private function withBooleanOption(string $name, bool $value): self
    {
        $integer = new Integer();
        $integer->setIval($value ? 1 : 0);

        $argNode = new Node();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $argNode->setInteger($integer);

        return $this->withOption($name, $argNode);
    }

    private function withIntegerOption(string $name, int $value): self
    {
        $integer = new Integer();
        $integer->setIval($value);

        $argNode = new Node();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $argNode->setInteger($integer);

        return $this->withOption($name, $argNode);
    }

    private function withOption(string $name, ?Node $arg): self
    {
        $newOptions = $this->options;
        $newOptions[] = ['name' => $name, 'arg' => $arg];

        return new self($this->name, $newOptions);
    }

    private function withStringOption(string $name, string $value): self
    {
        $str = new PBString();
        $str->setSval($value);

        $argNode = new Node();
        $argNode->setString($str);

        return $this->withOption($name, $argNode);
    }
}
