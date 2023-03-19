<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Serializer\Closure;
use Flow\ETL\Transformer;
use Laravel\SerializableClosure\SerializableClosure;

/**
 * @implements Transformer<array{callables: array<SerializableClosure>}>
 */
final class CallbackEntryTransformer implements Transformer
{
    /**
     * @phpstan-var array<callable(Entry) : Entry>
     */
    private readonly array $callables;

    /**
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
            $callables[] = $closure->getClosure();
        }

        /**
         * @psalm-suppress PropertyTypeCoercion
         * @psalm-suppress MixedPropertyTypeCoercion
         */
        $this->callables = $callables;
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @var callable(Row) : Row $transform
         */
        $transform = function (Row $row) : Row {
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
