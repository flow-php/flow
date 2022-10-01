<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DataFrameFactory;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Condition;
use Flow\ETL\Join\Join;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{factory: DataFrameFactory, condition: Condition, type: Join}>
 *
 * @psalm-immutable
 */
final class JoinEachRowsTransformer implements Transformer
{
    private function __construct(
        private readonly DataFrameFactory $factory,
        private readonly Condition $condition,
        private readonly Join $type
    ) {
    }

    /**
     * @psalm-pure
     */
    public static function inner(DataFrameFactory $right, Condition $condition) : self
    {
        return new self($right, $condition, Join::inner);
    }

    /**
     * @psalm-pure
     */
    public static function left(DataFrameFactory $right, Condition $condition) : self
    {
        return new self($right, $condition, Join::left);
    }

    /**
     * @psalm-pure
     */
    public static function leftAnti(DataFrameFactory $right, Condition $condition) : self
    {
        return new self($right, $condition, Join::left_anti);
    }

    /**
     * @psalm-pure
     */
    public static function right(DataFrameFactory $right, Condition $condition) : self
    {
        return new self($right, $condition, Join::right);
    }

    public function __serialize() : array
    {
        return [
            'factory' => $this->factory,
            'condition' => $this->condition,
            'type' => $this->type,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->factory = $data['factory'];
        $this->condition = $data['condition'];
        $this->type = $data['type'];
    }

    /**
     * @param FlowContext $context
     *
     * @psalm-suppress ImpureMethodCall
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
