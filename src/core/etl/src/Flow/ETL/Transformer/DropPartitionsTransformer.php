<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class DropPartitionsTransformer implements Transformer
{
    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        if ($rows->isPartitioned()) {
            return $rows->dropPartitions();
        }

        return $rows;
    }
}
