<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter;
use Flow\Types\Type\AutoCaster;

use function Flow\ETL\DSL\row;

final readonly class ScalarFunctionFilter implements Filter
{
    public function __construct(
        private ScalarFunction $function,
        private AutoCaster $caster,
        private FlowContext $context,
    ) {}

    public function accept(FileStatus $status): bool
    {
        $values = [];

        foreach ($status->path->partitions()->toArray() as $partition) {
            $values[$partition->name] = $this->caster->cast($partition->value);
        }

        // @mago-ignore analysis:mixed-operand
        return (bool) $this->function->eval(row($values), $this->context);
    }
}
