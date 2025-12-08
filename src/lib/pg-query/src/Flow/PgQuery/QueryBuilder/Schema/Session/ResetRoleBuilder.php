<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Session;

use Flow\PgQuery\Protobuf\AST\{VariableSetKind, VariableSetStmt};

final readonly class ResetRoleBuilder implements ResetRoleFinalStep
{
    private function __construct()
    {
    }

    public static function create() : ResetRoleFinalStep
    {
        return new self();
    }

    public function toAst() : VariableSetStmt
    {
        $stmt = new VariableSetStmt();
        $stmt->setKind(VariableSetKind::VAR_RESET);
        $stmt->setName('role');

        return $stmt;
    }
}
