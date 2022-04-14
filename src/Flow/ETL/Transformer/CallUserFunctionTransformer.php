<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Serializer\Closure;
use Flow\ETL\Transformer;
use Laravel\SerializableClosure\SerializableClosure;

/**
 * @implements Transformer<array{entries: array<string>, callback: callable, extra_arguments: array<mixed>, entry_factory: EntryFactory}>
 * @psalm-immutable
 */
final class CallUserFunctionTransformer implements Transformer
{
    /**
     * @psalm-var callable
     * @phpstan-var callable
     */
    private $callback;

    /**
     * @param array<string> $entries
     * @param array<mixed> $extraArguments
     */
    public function __construct(
        private readonly array $entries,
        callable $callback,
        private readonly array $extraArguments = [],
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
        $this->callback = $callback;
    }

    public function __serialize() : array
    {
        if ($this->callback instanceof \Closure && !Closure::isSerializable()) {
            throw new RuntimeException('CallUserFunctionTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'entries' => $this->entries,
            'callback' => $this->callback instanceof \Closure ? new SerializableClosure(\Closure::fromCallable($this->callback)) : $this->callback,
            'extra_arguments' => $this->extraArguments,
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        if ($this->callback instanceof \Closure && !Closure::isSerializable()) {
            throw new RuntimeException('CallUserFunctionTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->entries = $data['entries'];
        /** @psalm-suppress ImpureMethodCall */
        $this->callback = $data['callback'] instanceof SerializableClosure ? $data['callback']->getClosure() : $data['callback'];
        $this->extraArguments = $data['extra_arguments'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @var callable(Row) : Row $transform
         * @psalm-var pure-callable(Row) : Row $transform
         */
        $transform = function (Row $row) : Row {
            /** @psalm-var pure-callable(Row\Entry) : Row\Entry $entryMap */
            $entryMap = function (Row\Entry $entry) : Row\Entry {
                if (\in_array($entry->name(), $this->entries, true)) {
                    $entry = $this->entryFactory->create(
                        $entry->name(),
                        \call_user_func($this->callback, ...\array_merge([$entry->value()], $this->extraArguments))
                    );
                }

                return $entry;
            };
            $entries = $row->entries()->map($entryMap);

            return new Row(new Row\Entries(...$entries));
        };

        return $rows->map($transform);
    }
}
