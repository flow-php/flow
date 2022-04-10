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
 * @implements Transformer<array{object_entry_name: string, method: string, new_entry_name: string, parameters: array<mixed>, entry_factory: EntryFactory}>
 * @psalm-immutable
 */
final class ObjectMethodTransformer implements Transformer
{
    /**
     * ObjectMethodTransformer constructor.
     *
     * @param array<mixed> $parameters
     * @param EntryFactory $entryFactory
     */
    public function __construct(
        private readonly string $objectEntryName,
        private readonly string $method,
        private readonly string $newEntryName = 'method_entry',
        private readonly array $parameters = [],
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function __serialize() : array
    {
        return [
            'object_entry_name' => $this->objectEntryName,
            'method' => $this->method,
            'new_entry_name' => $this->newEntryName,
            'parameters' => $this->parameters,
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->objectEntryName = $data['object_entry_name'];
        $this->method = $data['method'];
        $this->newEntryName = $data['new_entry_name'];
        $this->parameters = $data['parameters'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @var callable(Row) : Row $transformer
         * @psalm-var pure-callable(Row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            if (!$row->entries()->has($this->objectEntryName)) {
                throw new RuntimeException("\"{$this->objectEntryName}\" entry not found");
            }

            if (!$row->entries()->get($this->objectEntryName) instanceof Row\Entry\ObjectEntry) {
                throw new RuntimeException("\"{$this->objectEntryName}\" entry is not ObjectEntry");
            }

            /**
             * @var object $object
             */
            $object = $row->get($this->objectEntryName)->value();

            if (!\method_exists($object, $this->method)) {
                throw new RuntimeException("\"{$this->objectEntryName}\" is object does not have \"{$this->method}\" method.");
            }

            return $row->add($this->entryFactory->create(
                $this->newEntryName,
                /** @phpstan-ignore-next-line */
                \call_user_func([$object, $this->method], ...$this->parameters)
            ));
        };

        return $rows->map($transformer);
    }
}
