<?php declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class LimitTransformer implements Transformer
{
    private int $rowsCount = 0;

    public function __construct(public readonly int $limit)
    {
        if ($this->limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: " . $this->limit);
        }
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $this->rowsCount += $rows->count();

        if ($this->rowsCount > $this->limit) {
            $rows = $rows->dropRight($this->rowsCount - $this->limit);

            if (\count($rows)) {
                return $rows;
            }

            throw new LimitReachedException($this->limit);
        }

        return $rows;
    }
}
