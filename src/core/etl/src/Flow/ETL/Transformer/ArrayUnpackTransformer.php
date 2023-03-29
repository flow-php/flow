<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{ref: string|EntryReference, skip_entry_names: array<string>, entry_factory: EntryFactory, entry_prefix: null|string}>
 */
final class ArrayUnpackTransformer implements Transformer
{
    /**
     * @param string[] $skipEntryNames
     */
    public function __construct(
        private readonly string|EntryReference $ref,
        private readonly array $skipEntryNames = [],
        private readonly ?string $entryPrefix = null,
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function __serialize() : array
    {
        return [
            'ref' => $this->ref,
            'skip_entry_names' => $this->skipEntryNames,
            'entry_factory' => $this->entryFactory,
            'entry_prefix' => $this->entryPrefix,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->ref = $data['ref'];
        $this->skipEntryNames = $data['skip_entry_names'];
        $this->entryFactory = $data['entry_factory'];
        $this->entryPrefix = $data['entry_prefix'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $rowsMap = function (Row $row) : Row {
            $arrayEntry = $row->entries()->get($this->ref);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                if ($this->ref instanceof EntryReference) {
                    throw new RuntimeException("\"{$this->ref->name()}\" is not ArrayEntry");
                }

                throw new RuntimeException("\"{$this->ref}\" is not ArrayEntry");
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
