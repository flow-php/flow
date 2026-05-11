<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Session;

use Flow\PostgreSql\Protobuf\AST\VariableSetKind;
use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class ResetRoleBuilder implements ResetRoleFinalStep
{
    use AstToSql;

    private function __construct() {}

    public static function create(): ResetRoleFinalStep
    {
        return new self();
    }

    public function toAst(): VariableSetStmt
    {
        $stmt = new VariableSetStmt();
        $stmt->setKind(VariableSetKind::VAR_RESET);
        $stmt->setName('role');

        return $stmt;
    }
}
