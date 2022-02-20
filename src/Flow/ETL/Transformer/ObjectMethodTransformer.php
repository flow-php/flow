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
 * @psalm-immutable
 */
final class ObjectMethodTransformer implements Transformer
{
    private string $objectEntryName;

    private string $method;

    private string $newEntryName;

    /**
     * @var array<mixed>
     */
    private array $parameters;

    private EntryFactory $entryFactory;

    /**
     * ObjectMethodTransformer constructor.
     *
     * @param string $objectEntryName
     * @param string $method
     * @param string $newEntryName
     * @param array<mixed> $parameters
     * @param null|EntryFactory $entryFactory
     */
    public function __construct(string $objectEntryName, string $method, string $newEntryName = 'method_entry', array $parameters = [], EntryFactory $entryFactory = null)
    {
        $this->objectEntryName = $objectEntryName;
        $this->method = $method;
        $this->newEntryName = $newEntryName;
        $this->parameters = $parameters;
        $this->entryFactory = null === $entryFactory ? new NativeEntryFactory() : $entryFactory;
    }

    /**
     * @return array{object_entry_name: string, method: string, new_entry_name: string, parameters: array<mixed>, entry_factory: EntryFactory}
     */
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

    /**
     * @param array{object_entry_name: string, method: string, new_entry_name: string, parameters: array<mixed>, entry_factory: EntryFactory} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
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
