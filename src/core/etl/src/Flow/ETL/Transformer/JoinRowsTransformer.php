<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DataFrame;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Condition;
use Flow\ETL\Join\Join;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{data_frame: ?DataFrame, condition: Condition, type: Join, rows: ?Rows}>
 *
 * @psalm-immutable
 */
final class JoinRowsTransformer implements Transformer
{
    private ?Rows $rows = null;

    private function __construct(
        private readonly DataFrame $dataFrame,
        private readonly Condition $condition,
        private readonly Join $type
    ) {
    }

    /**
     * @psalm-pure
     */
    public static function inner(DataFrame $right, Condition $condition) : self
    {
        return new self($right, $condition, Join::inner);
    }

    /**
     * @psalm-pure
     */
    public static function left(DataFrame $right, Condition $condition) : self
    {
        return new self($right, $condition, Join::left);
    }

    /**
     * @psalm-pure
     */
    public static function right(DataFrame $right, Condition $condition) : self
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
        /** @psalm-suppress ImpureMethodCall */
        $this->dataFrame = (new Flow())->process($rows);
        $this->condition = $data['condition'];
        $this->type = $data['type'];
        $this->rows = null;
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return match ($this->type) {
            Join::left => $rows->joinLeft($this->rows(), $this->condition),
            Join::right => $rows->joinRight($this->rows(), $this->condition),
            default => $rows->joinInner($this->rows(), $this->condition),
        };
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
