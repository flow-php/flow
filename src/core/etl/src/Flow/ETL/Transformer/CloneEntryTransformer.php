<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{from: string, to: string}>
 */
final class CloneEntryTransformer implements Transformer
{
    public function __construct(
        private readonly string $from,
        private readonly string $to
    ) {
    }

    public function __serialize() : array
    {
        return [
            'from' => $this->from,
            'to' => $this->to,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->from = $data['from'];
        $this->to = $data['to'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /** @psalm-var pure-callable(\Flow\ETL\Row) : \Flow\ETL\Row $clone */
        $clone = fn (Row $row) : Row => $row->add($row->get($this->from)->rename($this->to));

        return $rows->map($clone);
    }
}
