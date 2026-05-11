<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;

/**
 * Represents a window frame specification for window functions.
 */
final readonly class WindowFrame implements AstConvertible
{
    public function __construct(
        private FrameMode $mode,
        private FrameBound $startBound,
        private ?FrameBound $endBound = null,
        private FrameExclusion $exclusion = FrameExclusion::NO_OTHERS,
    ) {}

    public static function fromAst(Node $node): static
    {
        return new self(FrameMode::ROWS, FrameBound::currentRow());
    }

    public static function groups(FrameBound $start, ?FrameBound $end = null): self
    {
        return new self(FrameMode::GROUPS, $start, $end);
    }

    public static function range(FrameBound $start, ?FrameBound $end = null): self
    {
        return new self(FrameMode::RANGE, $start, $end);
    }

    public static function rows(FrameBound $start, ?FrameBound $end = null): self
    {
        return new self(FrameMode::ROWS, $start, $end);
    }

    public function endBound(): ?FrameBound
    {
        return $this->endBound;
    }

    public function exclusion(): FrameExclusion
    {
        return $this->exclusion;
    }

    public function mode(): FrameMode
    {
        return $this->mode;
    }

    public function startBound(): FrameBound
    {
        return $this->startBound;
    }

    public function toAst(): Node
    {
        return new Node();
    }

    public function withExclusion(FrameExclusion $exclusion): self
    {
        return new self($this->mode, $this->startBound, $this->endBound, $exclusion);
    }
}
