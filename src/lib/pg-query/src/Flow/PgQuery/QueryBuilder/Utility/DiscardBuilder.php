<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\DiscardStmt;

final readonly class DiscardBuilder implements DiscardFinalStep
{
    private function __construct(
        private DiscardType $type,
    ) {
    }

    public static function create(DiscardType $type) : DiscardFinalStep
    {
        return new self($type);
    }

    public function toAst() : DiscardStmt
    {
        $stmt = new DiscardStmt();
        $stmt->setTarget($this->type->value);

        return $stmt;
    }
}
