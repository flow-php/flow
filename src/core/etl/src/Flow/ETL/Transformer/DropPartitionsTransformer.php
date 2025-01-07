<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Rows, Transformer};

final readonly class DropPartitionsTransformer implements Transformer
{
    public function __construct(private bool $dropPartitionColumns = false)
    {

    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        if ($rows->isPartitioned()) {
            return $rows->dropPartitions($this->dropPartitionColumns);
        }

        return $rows;
    }
}
