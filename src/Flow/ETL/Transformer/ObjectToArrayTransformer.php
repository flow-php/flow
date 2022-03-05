<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Laminas\Hydrator\HydratorInterface;

/**
 * @implements Transformer<array{object_entry_name: string, hydrator: HydratorInterface}>
 * @psalm-immutable
 */
final class ObjectToArrayTransformer implements Transformer
{
    private HydratorInterface $hydrator;

    private string $objectEntryName;

    public function __construct(HydratorInterface $hydrator, string $objectEntryName)
    {
        $this->objectEntryName = $objectEntryName;
        $this->hydrator = $hydrator;
    }

    public function __serialize() : array
    {
        return [
            'object_entry_name' => $this->objectEntryName,
            'hydrator' => $this->hydrator,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->objectEntryName = $data['object_entry_name'];
        $this->hydrator = $data['hydrator'];
    }

    public function transform(Rows $rows) : Rows
    {
        /** @psalm-var pure-callable(Row) : Row $transformer */
        $transformer = function (Row $row) : Row {
            $entry = $row->entries()->get($this->objectEntryName);

            if (!$entry instanceof Row\Entry\ObjectEntry) {
                throw new RuntimeException("\"{$this->objectEntryName}\" is not ObjectEntry");
            }

            $entries = $row->entries()
                ->remove($this->objectEntryName)
                ->add(
                    new Row\Entry\ArrayEntry(
                        $this->objectEntryName,
                        $this->hydrator->extract(
                            $entry->value()
                        )
                    )
                );

            return new Row($entries);
        };

        return $rows->map($transformer);
    }
}
