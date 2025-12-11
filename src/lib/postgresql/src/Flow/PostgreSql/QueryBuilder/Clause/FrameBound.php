<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

/**
 * Represents a frame bound specification for window functions.
 */
final readonly class FrameBound implements AstConvertible
{
    private function __construct(
        private FrameBoundType $type,
        private ?Expression $offset = null,
    ) {
        if (($this->type === FrameBoundType::PRECEDING || $this->type === FrameBoundType::FOLLOWING) && $this->offset === null) {
            throw InvalidExpressionException::invalidValue('FrameBound offset', 'null for PRECEDING/FOLLOWING');
        }

        if (($this->type === FrameBoundType::CURRENT_ROW || $this->type === FrameBoundType::UNBOUNDED_PRECEDING || $this->type === FrameBoundType::UNBOUNDED_FOLLOWING) && $this->offset !== null) {
            throw InvalidExpressionException::invalidValue('FrameBound offset', 'non-null for CURRENT_ROW/UNBOUNDED');
        }
    }

    public static function currentRow() : self
    {
        return new self(FrameBoundType::CURRENT_ROW);
    }

    public static function following(Expression $offset) : self
    {
        return new self(FrameBoundType::FOLLOWING, $offset);
    }

    public static function fromAst(Node $node) : static
    {
        return new self(FrameBoundType::CURRENT_ROW);
    }

    public static function preceding(Expression $offset) : self
    {
        return new self(FrameBoundType::PRECEDING, $offset);
    }

    public static function unboundedFollowing() : self
    {
        return new self(FrameBoundType::UNBOUNDED_FOLLOWING);
    }

    public static function unboundedPreceding() : self
    {
        return new self(FrameBoundType::UNBOUNDED_PRECEDING);
    }

    public function offset() : ?Expression
    {
        return $this->offset;
    }

    public function toAst() : Node
    {
        if ($this->offset !== null) {
            return $this->offset->toAst();
        }

        return new Node();
    }

    public function type() : FrameBoundType
    {
        return $this->type;
    }
}
