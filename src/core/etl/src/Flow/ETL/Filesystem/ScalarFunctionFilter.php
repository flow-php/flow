<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use function Flow\ETL\DSL\row;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\PHP\Type\AutoCaster;
use Flow\ETL\Row\EntryFactory;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\{FileStatus, Partition};

final class ScalarFunctionFilter implements Filter
{
    public function __construct(
        private readonly ScalarFunction $function,
        private readonly EntryFactory $entryFactory,
        private readonly AutoCaster $caster,
    ) {
    }

    public function accept(FileStatus $status) : bool
    {
        return (bool) $this->function->eval(
            row(
                ...\array_map(
                    fn (Partition $partition) => $this->entryFactory->create($partition->name, $this->caster->cast($partition->value)),
                    $status->path->partitions()->toArray()
                )
            )
        );
    }
}
