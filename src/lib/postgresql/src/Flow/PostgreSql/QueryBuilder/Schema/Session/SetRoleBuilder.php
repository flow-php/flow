<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Session;

use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\VariableSetKind;
use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class SetRoleBuilder implements SetRoleFinalStep
{
    use AstToSql;

    private function __construct(
        private string $roleName,
    ) {}

    public static function create(string $role): SetRoleFinalStep
    {
        return new self($role);
    }

    public function toAst(): VariableSetStmt
    {
        $stmt = new VariableSetStmt();
        $stmt->setKind(VariableSetKind::VAR_SET_VALUE);
        $stmt->setName('role');

        $str = new PBString();
        $str->setSval($this->roleName);

        $aConst = new A_Const();
        $aConst->setSval($str);

        $node = new Node();
        $node->setAConst($aConst);

        $stmt->setArgs([$node]);

        return $stmt;
    }
}
