<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @psalm-immutable
 */
final class ArrayExpandTransformer implements Transformer
{
    private string $arrayEntryName;

    private string $expandEntryName;

    private EntryFactory $entryFactory;

    public function __construct(string $arrayEntryName, string $expandEntryName = 'element', EntryFactory $entryFactory = null)
    {
        $this->arrayEntryName = $arrayEntryName;
        $this->expandEntryName = $expandEntryName;
        $this->entryFactory = $entryFactory ? $entryFactory : new NativeEntryFactory();
    }

    /**
     * @return array{array_entry_name: string, expand_entry_name: string, entry_factory: EntryFactory}
     */
    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'expand_entry_name' => $this->expandEntryName,
            'entry_factory' => $this->entryFactory,
        ];
    }

    /**
     * @param array{array_entry_name: string, expand_entry_name: string, entry_factory: EntryFactory} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->expandEntryName = $data['expand_entry_name'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row[] $transformer
         */
        $transformer = function (Row $row) : array {
            $arrayEntry = $row->get($this->arrayEntryName);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                $entryClass = \get_class($arrayEntry);

                throw new RuntimeException("{$this->arrayEntryName} is not ArrayEntry but {$entryClass}");
            }

            $array = $arrayEntry->value();

            return \array_values(
                \array_map(
                    function ($arrayElement) use ($row) : Row {
                        return new Row(
                            $row->entries()
                                ->remove($this->arrayEntryName)
                                ->merge(new Entries($this->entryFactory->create($this->expandEntryName, $arrayElement)))
                        );
                    },
                    $array
                )
            );
        };

        return $rows->flatMap($transformer);
    }
}
