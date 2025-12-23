<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Cursor;

use Flow\PostgreSql\Protobuf\AST\ClosePortalStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class CloseCursorBuilder implements CloseCursorFinalStep
{
    use AstToSql;

    private function __construct(
        private string $cursorName,
    ) {
    }

    public static function close(string $cursorName) : CloseCursorFinalStep
    {
        return new self($cursorName);
    }

    public static function closeAll() : CloseCursorFinalStep
    {
        return new self('');
    }

    public function toAst() : ClosePortalStmt
    {
        $stmt = new ClosePortalStmt();
        $stmt->setPortalname($this->cursorName);

        return $stmt;
    }
}
