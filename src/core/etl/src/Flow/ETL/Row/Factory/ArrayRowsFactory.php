<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\RowsFactory;

/**
 * @implements RowsFactory<array{entry_factory: Row\EntryFactory}>
 */
final class ArrayRowsFactory implements RowsFactory
{
    public function __construct(
        private readonly Row\EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function __serialize() : array
    {
        return [
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryFactory = $data['entry_factory'];
    }

    /**
     * @param array<array<mixed>> $data
     *
     * @throws InvalidArgumentException
     * @throws \Flow\ETL\Exception\RuntimeException
     */
    public function create(array $data) : Rows
    {
        foreach ($data as $row) {
            /** @psalm-suppress DocblockTypeContradiction */
            if (!\is_array($row)) {
                throw new InvalidArgumentException('ArrayRowsFactory expects data to be an array of arrays');
            }
        }

        $rows = new Rows();

        foreach ($data as $dataRow) {
            $entries = [];

            /**
             * @var mixed $value
             */
            foreach ($dataRow as $entry => $value) {
                $entries[] = $this->entryFactory->create((string) $entry, $value);
            }

            $rows = $rows->add(Row::create(...$entries));
        }

        return $rows;
    }
}
