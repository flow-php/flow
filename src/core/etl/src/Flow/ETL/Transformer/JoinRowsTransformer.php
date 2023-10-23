<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DataFrame;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{data_frame: ?DataFrame, condition: Expression, type: Join, rows: ?Rows}>
 */
final class JoinRowsTransformer implements Transformer
{
    private ?Rows $rows = null;

    private function __construct(
        private readonly DataFrame $dataFrame,
        private readonly Expression $condition,
        private readonly Join $type
    ) {
    }

    public static function inner(DataFrame $right, Expression $condition) : self
    {
        return new self($right, $condition, Join::inner);
    }

    public static function left(DataFrame $right, Expression $condition) : self
    {
        return new self($right, $condition, Join::left);
    }

    public static function leftAnti(DataFrame $right, Expression $condition) : self
    {
        return new self($right, $condition, Join::left_anti);
    }

    public static function right(DataFrame $right, Expression $condition) : self
    {
        return new self($right, $condition, Join::right);
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
        $this->dataFrame = (new Flow())->process($rows);
        $this->condition = $data['condition'];
        $this->type = $data['type'];
        $this->rows = null;
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return match ($this->type) {
            Join::left => $rows->joinLeft($this->rows(), $this->condition),
            Join::left_anti => $rows->joinLeftAnti($this->rows(), $this->condition),
            Join::right => $rows->joinRight($this->rows(), $this->condition),
            default => $rows->joinInner($this->rows(), $this->condition),
        };
    }

    private function rows() : Rows
    {
        if ($this->rows === null) {
            $this->rows = $this->dataFrame->fetch();
        }

        return $this->rows;
    }
}
