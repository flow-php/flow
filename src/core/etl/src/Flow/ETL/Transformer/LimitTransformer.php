<?php

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{limit: int}>
 */
final class LimitTransformer implements Transformer
{
    private int $rowsCount = 0;

    public function __construct(private readonly int $limit)
    {
        if ($this->limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: " . $this->limit);
        }
    }

    public function __serialize(): array
    {
        return [
            'limit' => $this->limit,
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->limit = $data['limit'];
    }

    public function transform(Rows $rows, FlowContext $context): Rows
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