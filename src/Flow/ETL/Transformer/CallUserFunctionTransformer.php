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
use Opis\Closure\SerializableClosure;

/**
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
     * @var array<string>
     */
    private array $entries;

    private EntryFactory $entryFactory;

    /**
     * @param array<string> $entries
     * @param callable $callback
     */
    public function __construct(array $entries, callable $callback, EntryFactory $entryFactory = null)
    {
        $this->entries = $entries;
        $this->callback = $callback;
        $this->entryFactory = $entryFactory ?? new NativeEntryFactory();
    }

    /**
     * @return array{entries: array<string>, callback: callable, entry_factory: EntryFactory}
     */
    public function __serialize() : array
    {
        /** @psalm-suppress ImpureMethodCall */
        if ($this->callback instanceof \Closure && !Closure::isSerializable()) {
            throw new RuntimeException('CallUserFunctionTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'entries' => $this->entries,
            'callback' => $this->callback instanceof \Closure ? new SerializableClosure(\Closure::fromCallable($this->callback)) : $this->callback,
            'entry_factory' => $this->entryFactory,
        ];
    }

    /**
     * @param array{entries: array<string>, callback: callable, entry_factory: EntryFactory} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        /** @psalm-suppress ImpureMethodCall */
        if ($this->callback instanceof \Closure && !Closure::isSerializable()) {
            throw new RuntimeException('CallUserFunctionTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->entries = $data['entries'];
        $this->callback = $data['callback'] instanceof SerializableClosure ? $data['callback']->getClosure() : $data['callback'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @var callable(Row) : Row $transform
         * @psalm-var pure-callable(Row) : Row $transform
         */
        $transform = function (Row $row) : Row {
            $entries = $row->entries()->map(function (Row\Entry $entry) : Row\Entry {
                if (\in_array($entry->name(), $this->entries, true)) {
                    $entry = $this->entryFactory->create(
                        $entry->name(),
                        \call_user_func($this->callback, $entry->value())
                    );
                }

                return $entry;
            });

            return new Row(new Row\Entries(...$entries));
        };

        return $rows->map($transform);
    }
}
