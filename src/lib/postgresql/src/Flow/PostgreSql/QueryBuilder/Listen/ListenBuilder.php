<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Listen;

use Flow\PostgreSql\Protobuf\AST\ListenStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class ListenBuilder implements ListenFinalStep
{
    use AstToSql;

    private function __construct(
        private string $channel,
    ) {}

    public static function create(string $channel): ListenFinalStep
    {
        return new self($channel);
    }

    public function toAst(): ListenStmt
    {
        return (new ListenStmt())->setConditionname($this->channel);
    }
}
