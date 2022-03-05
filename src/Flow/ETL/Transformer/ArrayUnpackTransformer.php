<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{array_entry_name: string, skip_entry_names: array<string>, entry_factory: EntryFactory, entry_prefix: null|string}>
 * @psalm-immutable
 */
final class ArrayUnpackTransformer implements Transformer
{
    private string $arrayEntryName;

    private EntryFactory $entryFactory;

    private ?string $entryPrefix;

    /**
     * @var string[]
     */
    private array $skipEntryNames;

    /**
     * @param string[] $skipEntryNames
     */
    public function __construct(string $arrayEntryName, array $skipEntryNames = [], ?string $entryPrefix = null, EntryFactory $entryFactory = null)
    {
        $this->arrayEntryName = $arrayEntryName;
        $this->skipEntryNames = $skipEntryNames;
        $this->entryFactory = $entryFactory ?? new NativeEntryFactory();
        $this->entryPrefix = $entryPrefix;
    }

    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'skip_entry_names' => $this->skipEntryNames,
            'entry_factory' => $this->entryFactory,
            'entry_prefix' => $this->entryPrefix,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->skipEntryNames = $data['skip_entry_names'];
        $this->entryFactory = $data['entry_factory'];
        $this->entryPrefix = $data['entry_prefix'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row) : Row $rowsMap
         */
        $rowsMap = function (Row $row) : Row {
            $arrayEntry = $row->entries()->get($this->arrayEntryName);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                throw new RuntimeException("\"{$this->arrayEntryName}\" is not ArrayEntry");
            }

            $entries = [];
            /**
             * @var int|string $key
             * @var mixed $value
             */
            foreach ($arrayEntry->value() as $key => $value) {
                $entryName = (string) $key;

                if (\in_array($entryName, $this->skipEntryNames, true)) {
                    continue;
                }

                if ($this->entryPrefix) {
                    $entryName = $this->entryPrefix . $entryName;
                }

                $entries[] = $this->entryFactory->create($entryName, $value);
            }

            if (\count($entries)) {
                return new Row($row->entries()->merge(new Row\Entries(...$entries)));
            }

            return $row;
        };

        return $rows->map($rowsMap);
    }
}
