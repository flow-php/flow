<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\DiscardStmt;
use Flow\PostgreSql\QueryBuilder\AstDeparser;

final readonly class DiscardBuilder implements DiscardFinalStep
{
    private function __construct(
        private DiscardType $type,
    ) {}

    public static function create(DiscardType $type): DiscardFinalStep
    {
        return new self($type);
    }

    public function toSql(): string
    {
        return (new AstDeparser())->deparse($this->toAst());
    }

    public function toAst(): DiscardStmt
    {
        $stmt = new DiscardStmt();
        $stmt->setTarget($this->type->value);

        return $stmt;
    }
}
