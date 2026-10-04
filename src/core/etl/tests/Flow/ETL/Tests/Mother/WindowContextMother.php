<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Window\WholePartitionFrame;
use Flow\ETL\Window\WindowContext;
use Flow\ETL\Window\WindowFrame;

use function Flow\ETL\DSL\flow_context;

final class WindowContextMother
{
    public static function atIndex(
        Rows $partition,
        int $index,
        ?WindowFrame $frame = null,
        ?FlowContext $context = null,
    ): WindowContext {
        return new WindowContext($index, $partition, $frame ?? new WholePartitionFrame(), $context ?? flow_context());
    }
}
