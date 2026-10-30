<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Unlisten;

use Flow\PostgreSql\Protobuf\AST\UnlistenStmt;
use Flow\PostgreSql\QueryBuilder\AstDeparser;

final readonly class UnlistenBuilder implements UnlistenFinalStep
{
    private function __construct(
        private string $channel,
    ) {}

    /**
     * Does not support the unquoted `UNLISTEN *` wildcard. libpg_query treats
     * `conditionname = "*"` as a literal channel name and emits `UNLISTEN "*"`.
     * Pass a specific channel name instead.
     */
    public static function create(string $channel): UnlistenFinalStep
    {
        return new self($channel);
    }

    public function toSql(): string
    {
        return (new AstDeparser())->deparse($this->toAst());
    }

    public function toAst(): UnlistenStmt
    {
        return (new UnlistenStmt())->setConditionname($this->channel);
    }
}
