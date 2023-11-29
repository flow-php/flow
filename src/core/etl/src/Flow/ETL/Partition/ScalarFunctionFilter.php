<?php declare(strict_types=1);

namespace Flow\ETL\Partition;

use function Flow\ETL\DSL\row;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Partition;
use Flow\ETL\Row\EntryFactory;

/**
 * @implements PartitionFilter<array{function: ScalarFunction, entry_factory: EntryFactory}>
 */
final class ScalarFunctionFilter implements PartitionFilter
{
    public function __construct(
        private readonly ScalarFunction $function,
        private readonly EntryFactory $entryFactory
    ) {
    }

    public function __serialize() : array
    {
        return [
            'function' => $this->function,
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->function = $data['function'];
        $this->entryFactory = $data['entry_factory'];
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
