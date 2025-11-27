<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Visitors;

use Flow\PgQuery\AST\NodeVisitor;
use Flow\PgQuery\Protobuf\AST\FuncCall;

/**
 * A visitor that collects all FuncCall (function call) nodes.
 */
final class FuncCallCollector implements NodeVisitor
{
    /**
     * @var array<FuncCall>
     */
    private array $funcCalls = [];

    public static function nodeClass() : string
    {
        return FuncCall::class;
    }

    public function enter(object $node) : ?int
    {
        /** @var FuncCall $node */
        $this->funcCalls[] = $node;

        return null;
    }

    /**
     * @return array<FuncCall>
     */
    public function getFuncCalls() : array
    {
        return $this->funcCalls;
    }

    public function leave(object $node) : ?int
    {
        return null;
    }

    public function reset() : void
    {
        $this->funcCalls = [];
    }
}
