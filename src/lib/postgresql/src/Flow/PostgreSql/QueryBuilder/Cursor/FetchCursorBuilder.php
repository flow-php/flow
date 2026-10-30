<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Cursor;

use Flow\PostgreSql\Protobuf\AST\FetchDirection;
use Flow\PostgreSql\Protobuf\AST\FetchStmt;
use Flow\PostgreSql\QueryBuilder\AstDeparser;

final class FetchCursorBuilder implements FetchCursorFinalStep
{
    private int $count = 1;

    private int $direction = FetchDirection::FETCH_FORWARD;

    private function __construct(
        private readonly string $cursorName,
    ) {}

    public static function create(string $cursorName): self
    {
        return new self($cursorName);
    }

    public function all(): self
    {
        $this->count = 0;
        $this->direction = FetchDirection::FETCH_FORWARD;

        return $this;
    }

    public function backward(int $count = 1): self
    {
        $this->count = $count;
        $this->direction = FetchDirection::FETCH_BACKWARD;

        return $this;
    }

    public function forward(int $count = 1): self
    {
        $this->count = $count;
        $this->direction = FetchDirection::FETCH_FORWARD;

        return $this;
    }

    public function next(): self
    {
        $this->count = 1;
        $this->direction = FetchDirection::FETCH_FORWARD;

        return $this;
    }

    public function prior(): self
    {
        $this->count = 1;
        $this->direction = FetchDirection::FETCH_BACKWARD;

        return $this;
    }

    public function toSql(): string
    {
        return (new AstDeparser())->deparse($this->toAst());
    }

    public function toAst(): FetchStmt
    {
        $stmt = new FetchStmt();
        $stmt->setPortalname($this->cursorName);
        $stmt->setDirection($this->direction);
        $stmt->setHowMany($this->count);
        $stmt->setIsmove(false);

        return $stmt;
    }
}
