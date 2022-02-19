<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Factory\NativeEntryFactory;

/**
 * @psalm-immutable
 */
final class ArrayUnpackTransformer implements Transformer
{
    private string $arrayEntryName;

    /**
     * @var string[]
     */
    private array $skipEntryNames;

    private ?string $entryPrefix;

    private EntryFactory $entryFactory;

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

    /**
     * @return array{array_entry_name: string, skip_entry_names: array<string>, entry_factory: EntryFactory, entry_prefix: null|string}
     */
    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'skip_entry_names' => $this->skipEntryNames,
            'entry_factory' => $this->entryFactory,
            'entry_prefix' => $this->entryPrefix,
        ];
    }

    /**
     * @param array{array_entry_name: string, skip_entry_names: array<string>, entry_factory: EntryFactory, entry_prefix: null|string} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->skipEntryNames = $data['skip_entry_names'];
        $this->entryFactory = $data['entry_factory'];
        $this->entryPrefix = $data['entry_prefix'];
    }

    /**
     * @psalm-suppress InvalidArgument
     * @psalm-suppress InvalidScalarArgument
     * @psalm-suppress MixedArgument
     */
    public function transform(Rows $rows) : Rows
    {
        return $rows->map(function (Row $row) : Row {
            if (!$row->entries()->has($this->arrayEntryName)) {
                throw new RuntimeException("\"{$this->arrayEntryName}\" not found");
            }

            if (!$row->entries()->get($this->arrayEntryName) instanceof Row\Entry\ArrayEntry) {
                throw new RuntimeException("\"{$this->arrayEntryName}\" is not ArrayEntry");
            }

            $entries = [];
            /**
             * @var int|string $key
             * @var mixed $value
             */
            foreach ($row->valueOf($this->arrayEntryName) as $key => $value) {
                $entryName = (string) $key;

                if (\in_array($entryName, $this->skipEntryNames, true)) {
                    continue;
                }

                if ($this->entryPrefix) {
                    $entryName = $this->entryPrefix . $entryName;
                }

                $entries[] = $this->entryFactory->createEntry($entryName, $value);
            }

            if (\count($entries)) {
                return new Row($row->entries()->merge(new Row\Entries(...$entries)));
            }

            return $row;
        });
    }
}
