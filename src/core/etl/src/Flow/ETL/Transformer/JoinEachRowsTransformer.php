<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Join\{Expression, Join};
use Flow\ETL\{DataFrameFactory, FlowContext, Rows, Transformer};

final readonly class JoinEachRowsTransformer implements Transformer
{
    private function __construct(
        private DataFrameFactory $factory,
        private Expression $expression,
        private Join $type,
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
        $rightRows = $this->factory->from($rows)->fetch();

        return match ($this->type) {
            Join::left => $rows->joinLeft($rightRows, $this->expression),
            Join::left_anti => $rows->joinLeftAnti($rightRows, $this->expression),
            Join::right => $rows->joinRight($rightRows, $this->expression),
            default => $rows->joinInner($rightRows, $this->expression),
        };
    }
}
