<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Opis\Closure\SerializableClosure;

/**
 * @psalm-immutable
 */
final class CallbackEntryTransformer implements Transformer
{
    private static ?bool $isSerializable = null;

    /**
     * @psalm-var array<pure-callable(Entry) : Entry>
     * @phpstan-var array<callable(Entry) : Entry>
     */
    private array $callables;

    /**
     * @psalm-param pure-callable(Entry) : Entry ...$callables
     *
     * @param callable(Entry) : Entry ...$callables
     */
    public function __construct(callable ...$callables)
    {
        $this->callables = $callables;
    }

    /**
     * @return array{callables: array<SerializableClosure>}
     */
    public function __serialize() : array
    {
        /** @psalm-suppress ImpureMethodCall */
        if (!self::isSerializable()) {
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

    /**
     * @param array{callables: array<SerializableClosure>} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        /** @psalm-suppress ImpureMethodCall */
        if (!self::isSerializable()) {
            throw new RuntimeException('CallbackEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $callables = [];

        foreach ($data['callables'] as $closure) {
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
            $entries = $row->entries()->map(function (Row\Entry $entry) : Row\Entry {
                foreach ($this->callables as $callable) {
                    $entry = $callable($entry);
                }

                return $entry;
            });

            return new Row(new Row\Entries(...$entries));
        };

        return $rows->map($transform);
    }

    private static function isSerializable() : bool
    {
        if (self::$isSerializable === null) {
            self::$isSerializable = \class_exists('Opis\Closure\SerializableClosure');
        }

        return self::$isSerializable;
    }
}
