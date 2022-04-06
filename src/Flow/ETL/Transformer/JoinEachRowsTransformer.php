<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DataFrameFactory;
use Flow\ETL\Join\Condition;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{factory: DataFrameFactory, condition: Condition, type: "left"|"right"|"inner"}>
 * @psalm-immutable
 */
final class JoinEachRowsTransformer implements Transformer
{
    private Condition $condition;

    private DataFrameFactory $factory;

    /**
     * @var string
     * @psalm-var "left"|"right"|"inner"
     */
    private string $type;

    /**
     * @param DataFrameFactory $factory
     * @param Condition $condition
     * @param string $type
     * @psalm-param "left"|"right"|"inner" $type
     */
    private function __construct(DataFrameFactory $factory, Condition $condition, string $type)
    {
        $this->factory = $factory;
        $this->condition = $condition;
        $this->type = $type;
    }

    /**
     * @psalm-pure
     *
     * @param DataFrameFactory $right
     * @param Condition $condition
     *
     * @return self
     */
    public static function inner(DataFrameFactory $right, Condition $condition) : self
    {
        return new self($right, $condition, 'inner');
    }

    /**
     * @psalm-pure
     *
     * @param DataFrameFactory $right
     * @param Condition $condition
     *
     * @return self
     */
    public static function left(DataFrameFactory $right, Condition $condition) : self
    {
        return new self($right, $condition, 'left');
    }

    /**
     * @psalm-pure
     *
     * @param DataFrameFactory $right
     * @param Condition $condition
     *
     * @return self
     */
    public static function right(DataFrameFactory $right, Condition $condition) : self
    {
        return new self($right, $condition, 'right');
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
     * @psalm-suppress ImpureMethodCall
     *
     * @param Rows $rows
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return Rows
     */
    public function transform(Rows $rows) : Rows
    {
        switch ($this->type) {
            case 'left':
                return $rows->joinLeft($this->factory->from($rows)->fetch(), $this->condition);
            case 'right':
                return $rows->joinRight($this->factory->from($rows)->fetch(), $this->condition);

            default:
                return $rows->joinInner($this->factory->from($rows)->fetch(), $this->condition);
        }
    }
}
