<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DataFrame;
use Flow\ETL\Flow;
use Flow\ETL\Join\Condition;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{data_frame: ?DataFrame, condition: Condition, type: "left"|"right"|"inner", rows: ?Rows}>
 * @psalm-immutable
 */
final class JoinRowsTransformer implements Transformer
{
    private Condition $condition;

    private DataFrame $dataFrame;

    private ?Rows $rows;

    /**
     * @var string
     * @psalm-var "left"|"right"|"inner"
     */
    private string $type;

    /**
     * @param DataFrame $dataFrame
     * @param Condition $condition
     * @param string $type
     * @psalm-param "left"|"right"|"inner" $type
     */
    private function __construct(DataFrame $dataFrame, Condition $condition, string $type)
    {
        $this->dataFrame = $dataFrame;
        $this->rows = null;
        $this->condition = $condition;
        $this->type = $type;
    }

    /**
     * @psalm-pure
     *
     * @param DataFrame $right
     * @param Condition $condition
     *
     * @return self
     */
    public static function inner(DataFrame $right, Condition $condition) : self
    {
        return new self($right, $condition, 'inner');
    }

    /**
     * @psalm-pure
     *
     * @param DataFrame $right
     * @param Condition $condition
     *
     * @return self
     */
    public static function left(DataFrame $right, Condition $condition) : self
    {
        return new self($right, $condition, 'left');
    }

    /**
     * @psalm-pure
     *
     * @param DataFrame $right
     * @param Condition $condition
     *
     * @return self
     */
    public static function right(DataFrame $right, Condition $condition) : self
    {
        return new self($right, $condition, 'right');
    }

    public function __serialize() : array
    {
        return [
            'data_frame' => null,
            'condition' => $this->condition,
            'type' => $this->type,
            'rows' => $this->rows(),
        ];
    }

    public function __unserialize(array $data) : void
    {
        /** @var Rows $rows */
        $rows = $data['rows'];
        /** @psalm-suppress ImpureMethodCall */
        $this->dataFrame = (new Flow())->process($rows);
        $this->condition = $data['condition'];
        $this->type = $data['type'];
        $this->rows = null;
    }

    public function transform(Rows $rows) : Rows
    {
        switch ($this->type) {
            case 'left':
                return $rows->joinLeft($this->rows(), $this->condition);
            case 'right':
                return $rows->joinRight($this->rows(), $this->condition);

            default:
                return $rows->joinInner($this->rows(), $this->condition);
        }
    }

    /**
     * @psalm-suppress InvalidNullableReturnType
     * @psalm-suppress NullableReturnStatement
     * @psalm-suppress InaccessibleProperty
     * @psalm-suppress ImpureMethodCall
     */
    private function rows() : Rows
    {
        if ($this->rows === null) {
            $this->rows = $this->dataFrame->fetch();
        }

        return $this->rows;
    }
}
