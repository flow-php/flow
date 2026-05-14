<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

use Flow\PostgreSql\Protobuf\AST\CreateRoleStmt;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RoleSpec;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\Protobuf\AST\RoleStmtType;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class CreateRoleBuilder implements CreateRoleFinalStep, CreateRoleOptionsStep
{
    use AstToSql;

    /**
     * @param list<array{name: string, arg: ?Node}> $options
     * @param list<string> $inRoles
     */
    private function __construct(
        private string $name,
        private array $options = [],
        private array $inRoles = [],
    ) {}

    public static function create(string $name): CreateRoleOptionsStep
    {
        return new self($name);
    }

    public function bypassRls(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::BYPASSRLS->value, true);
    }

    public function connectionLimit(int $limit): CreateRoleOptionsStep
    {
        return $this->withIntegerOption('connectionlimit', $limit);
    }

    public function createDb(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::CREATEDB->value, true);
    }

    public function createRole(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::CREATEROLE->value, true);
    }

    public function inherit(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::INHERIT->value, true);
    }

    public function inRole(string ...$roles): CreateRoleOptionsStep
    {
        return new self($this->name, $this->options, \array_values($roles));
    }

    public function login(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::LOGIN->value, true);
    }

    public function noBypassRls(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::BYPASSRLS->value, false);
    }

    public function noCreateDb(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::CREATEDB->value, false);
    }

    public function noCreateRole(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::CREATEROLE->value, false);
    }

    public function noInherit(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::INHERIT->value, false);
    }

    public function noLogin(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::LOGIN->value, false);
    }

    public function noReplication(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::REPLICATION->value, false);
    }

    public function noSuperuser(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::SUPERUSER->value, false);
    }

    public function replication(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::REPLICATION->value, true);
    }

    public function superuser(): CreateRoleOptionsStep
    {
        return $this->withBooleanOption(RoleOption::SUPERUSER->value, true);
    }

    public function toAst(): CreateRoleStmt
    {
        $stmt = new CreateRoleStmt();
        $stmt->setStmtType(RoleStmtType::ROLESTMT_ROLE);
        $stmt->setRole($this->name);

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

        if ($this->inRoles !== []) {
            $roleNodes = [];

            foreach ($this->inRoles as $roleName) {
                $roleSpec = new RoleSpec();
                $roleSpec->setRoletype(RoleSpecType::ROLESPEC_CSTRING);
                $roleSpec->setRolename($roleName);

                $node = new Node();
                $node->setRoleSpec($roleSpec);
                $roleNodes[] = $node;
            }

            $defElem = new DefElem();
            $defElem->setDefname('addroleto');

            $listNode = new Node();
            $list = new PBList();
            $list->setItems($roleNodes);
            $listNode->setList($list);
            $defElem->setArg($listNode);

            $node = new Node();
            $node->setDefElem($defElem);
            $optionNodes[] = $node;
        }

        if ($optionNodes !== []) {
            $stmt->setOptions($optionNodes);
        }

        return $stmt;
    }

    public function validUntil(string $timestamp): CreateRoleOptionsStep
    {
        return $this->withStringOption('validUntil', $timestamp);
    }

    public function withPassword(#[\SensitiveParameter] string $password): CreateRoleOptionsStep
    {
        return $this->withStringOption('password', $password);
    }

    private function withBooleanOption(string $name, bool $value): self
    {
        $integer = new Integer();
        $integer->setIval($value ? 1 : 0);

        $argNode = new Node(['integer' => $integer]);

        return $this->withOption($name, $argNode);
    }

    private function withIntegerOption(string $name, int $value): self
    {
        $integer = new Integer();
        $integer->setIval($value);

        $argNode = new Node(['integer' => $integer]);

        return $this->withOption($name, $argNode);
    }

    private function withOption(string $name, ?Node $arg): self
    {
        $newOptions = $this->options;
        $newOptions[] = ['name' => $name, 'arg' => $arg];

        return new self($this->name, $newOptions, $this->inRoles);
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
