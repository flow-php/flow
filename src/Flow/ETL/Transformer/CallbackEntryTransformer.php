<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Serializer\Closure;
use Flow\ETL\Transformer;
use Laravel\SerializableClosure\SerializableClosure;

/**
 * @implements Transformer<array{callables: array<SerializableClosure>}>
 * @psalm-immutable
 */
final class CallbackEntryTransformer implements Transformer
{
    /**
     * @psalm-var array<pure-callable(Entry) : Entry>
     * @phpstan-var array<callable(Entry) : Entry>
     */
    private readonly array $callables;

    /**
     * @psalm-param pure-callable(Entry) : Entry ...$callables
     *
     * @param callable(Entry) : Entry ...$callables
     */
    public function __construct(callable ...$callables)
    {
        $this->callables = $callables;
    }

    public function __serialize() : array
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $closures = [];

        foreach ($this->callables as $callable) {
            $closures[] = new SerializableClosure(\Closure::fromCallable($callable));
        }

        return [
            'callables' => $closures,
        ];
    }

    public function __unserialize(array $data) : void
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $callables = [];

        foreach ($data['callables'] as $closure) {
            /** @psalm-suppress ImpureMethodCall */
            $callables[] = $closure->getClosure();
        }

        $this->callables = $callables;
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @var callable(Row) : Row $transform
         * @psalm-var pure-callable(Row) : Row $transform
         */
        $transform = function (Row $row) : Row {
            /** @psalm-var pure-callable(Row\Entry) : Row\Entry $callable */
            $callable = function (Row\Entry $entry) : Row\Entry {
                foreach ($this->callables as $callable) {
                    $entry = $callable($entry);
                }

                return $entry;
            };
            $entries = $row->entries()->map($callable);

            return new Row(new Row\Entries(...$entries));
        };

        return $rows->map($transform);
    }
}
