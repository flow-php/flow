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
 * @implements Transformer<array{ref: EntryReference, method: string, new_entry_name: string, parameters: array<mixed>, entry_factory: EntryFactory}>
 */
final class ObjectMethodTransformer implements Transformer
{
    private readonly EntryReference $ref;

    /**
     * ObjectMethodTransformer constructor.
     *
     * @param array<mixed> $parameters
     * @param EntryFactory $entryFactory
     */
    public function __construct(
        string|EntryReference $ref,
        private readonly string $method,
        private readonly string $newEntryName = 'method_entry',
        private readonly array $parameters = [],
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
        $this->ref = EntryReference::init($ref);
    }

    public function __serialize() : array
    {
        return [
            'ref' => $this->ref,
            'method' => $this->method,
            'new_entry_name' => $this->newEntryName,
            'parameters' => $this->parameters,
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->ref = $data['ref'];
        $this->method = $data['method'];
        $this->newEntryName = $data['new_entry_name'];
        $this->parameters = $data['parameters'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @var callable(Row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            if (!$row->entries()->has($this->ref)) {
                throw new RuntimeException("\"{$this->ref->name()}\" entry not found");
            }

            if (!$row->entries()->get($this->ref) instanceof Row\Entry\ObjectEntry) {
                throw new RuntimeException("\"{$this->ref->name()}\" entry is not ObjectEntry");
            }

            /**
             * @var object $object
             */
            $object = $row->get($this->ref)->value();

            if (!\method_exists($object, $this->method)) {
                throw new RuntimeException("\"{$this->ref->name()}\" is object does not have \"{$this->method}\" method.");
            }

            return $row->set($this->entryFactory->create(
                $this->newEntryName,
                /** @phpstan-ignore-next-line */
                \call_user_func([$object, $this->method], ...$this->parameters)
            ));
        };

        return $rows->map($transformer);
    }
}
