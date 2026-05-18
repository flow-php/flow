<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row\EntryFactory;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path\Filter;
use Flow\Types\Type\AutoCaster;

use function array_map;
use function Flow\ETL\DSL\row;

final readonly class ScalarFunctionFilter implements Filter
{
    public function __construct(
        private ScalarFunction $function,
        private EntryFactory $entryFactory,
        private AutoCaster $caster,
        private FlowContext $context,
    ) {}

    public function accept(FileStatus $status): bool
    {
        return (bool) $this->function->eval(
            row(...array_map(fn(Partition $partition) => $this->entryFactory->create(
                $partition->name,
                $this->caster->cast($partition->value),
            ), $status->path->partitions()->toArray())),
            $this->context,
        );
    }
}
