<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Laminas\Hydrator\HydratorInterface;

/**
 * @implements Transformer<array{ref: string|EntryReference, hydrator: HydratorInterface}>
 */
final class ObjectToArrayTransformer implements Transformer
{
    public function __construct(
        private readonly HydratorInterface $hydrator,
        private readonly string|EntryReference $ref
    ) {
    }

    public function __serialize() : array
    {
        return [
            'ref' => $this->ref,
            'hydrator' => $this->hydrator,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->ref = $data['ref'];
        $this->hydrator = $data['hydrator'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : Row {
            $entry = $row->entries()->get($this->ref);

            if (!$entry instanceof Row\Entry\ObjectEntry) {
                if ($this->ref instanceof EntryReference) {
                    throw new RuntimeException("\"{$this->ref->name()}\" is not ObjectEntry");
                }

                throw new RuntimeException("\"{$this->ref}\" is not ObjectEntry");
            }

            $entries = $row->entries()
                ->set(
                    new Row\Entry\ArrayEntry(
                        $this->ref instanceof EntryReference ? $this->ref->name() : $this->ref,
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
