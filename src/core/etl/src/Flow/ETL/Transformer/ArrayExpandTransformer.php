<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{array_entry_name: string, expand_entry_name: string, entry_factory: EntryFactory}>
 */
final class ArrayExpandTransformer implements Transformer
{
    public function __construct(
        private readonly string $arrayEntryName,
        private readonly string $expandEntryName = 'element',
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'expand_entry_name' => $this->expandEntryName,
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->expandEntryName = $data['expand_entry_name'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : array {
            $arrayEntry = $row->get($this->arrayEntryName);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry && !$arrayEntry instanceof Row\Entry\ListEntry) {
                $entryClass = $arrayEntry::class;

                throw new RuntimeException("{$this->arrayEntryName} is not ArrayEntry but {$entryClass}");
            }

            $array = $arrayEntry->value();

            return \array_values(
                \array_map(
                    fn ($arrayElement) : Row => new Row(
                        $row->entries()
                            ->remove($this->arrayEntryName)
                            ->merge(new Entries($this->entryFactory->create($this->expandEntryName, $arrayElement)))
                    ),
                    $array
                )
            );
        };

        return $rows->flatMap($transformer);
    }
}
