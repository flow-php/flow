<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Session;

use Flow\PgQuery\Protobuf\AST\{A_Const, Node, PBString, VariableSetKind, VariableSetStmt};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class SetRoleBuilder implements SetRoleFinalStep
{
    use AstToSql;

    private function __construct(
        private string $roleName,
    ) {
    }

    public static function create(string $role) : SetRoleFinalStep
    {
        return new self($role);
    }

    public function toAst() : VariableSetStmt
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
