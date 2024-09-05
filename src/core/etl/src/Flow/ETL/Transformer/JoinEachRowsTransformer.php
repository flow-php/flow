<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Join\{Expression, Join};
use Flow\ETL\{DataFrameFactory, FlowContext, Rows, Transformer};

final class JoinEachRowsTransformer implements Transformer
{
    private function __construct(
        private readonly DataFrameFactory $factory,
        private readonly Expression $condition,
        private readonly Join $type,
    ) {
    }

    public static function inner(DataFrameFactory $right, Expression $condition) : self
    {
        return new self($right, $condition, Join::inner);
    }

    public static function left(DataFrameFactory $right, Expression $condition) : self
    {
        return new self($right, $condition, Join::left);
    }

    public static function leftAnti(DataFrameFactory $right, Expression $condition) : self
    {
        return new self($right, $condition, Join::left_anti);
    }

    public static function right(DataFrameFactory $right, Expression $condition) : self
    {
        return new self($right, $condition, Join::right);
    }

    /**
     * @param FlowContext $context
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     */
    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return match ($this->type) {
            Join::left => $rows->joinLeft($this->factory->from($rows)->fetch(), $this->condition),
            Join::left_anti => $rows->joinLeftAnti($this->factory->from($rows)->fetch(), $this->condition),
            Join::right => $rows->joinRight($this->factory->from($rows)->fetch(), $this->condition),
            default => $rows->joinInner($this->factory->from($rows)->fetch(), $this->condition),
        };
    }
}
