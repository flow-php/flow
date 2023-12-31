<?php declare(strict_types=1);

namespace Flow\ETL\Partition;

use function Flow\ETL\DSL\row;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Partition;
use Flow\ETL\Row\EntryFactory;

final class ScalarFunctionFilter implements PartitionFilter
{
    public function __construct(
        private readonly ScalarFunction $function,
        private readonly EntryFactory $entryFactory
    ) {
    }

    public function keep(Partition ...$partitions) : bool
    {
        try {
            return (bool) $this->function->eval(
                row(
                    ...\array_map(
                        fn (Partition $partition) => $this->entryFactory->create($partition->name, $partition->value),
                        $partitions
                    )
                )
            );
        } catch (\Exception $e) {
            return false;
        }
    }
}
