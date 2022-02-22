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
final class EntryValueTransformer implements Transformer
{
    private static ?bool $isSerializable = null;

    /**
     * @var array<string>
     */
    private array $entries;

    private string $userFunction;

    private EntryFactory $entryFactory;

    /**
     * @param array<string> $entries
     * @param string $userFunction
     */
    public function __construct(array $entries, string $userFunction, EntryFactory $entryFactory = null)
    {
        $this->entries = $entries;
        $this->userFunction = $userFunction;
        $this->entryFactory = $entryFactory ?? new NativeEntryFactory();
    }

    /**
     * @return array{entries: array<string>, user_function: string, entry_factory: EntryFactory}
     */
    public function __serialize() : array
    {
        /** @psalm-suppress ImpureMethodCall */
        if (!self::isSerializable()) {
            throw new RuntimeException('CallbackEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'entries' => $this->entries,
            'user_function' => $this->userFunction,
            'entry_factory' => $this->entryFactory,
        ];
    }

    /**
     * @param array{entries: array<string>, user_function: string, entry_factory: EntryFactory} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        /** @psalm-suppress ImpureMethodCall */
        if (!self::isSerializable()) {
            throw new RuntimeException('CallbackEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->entries = $data['entries'];
        $this->userFunction = $data['user_function'];
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
                        /** @phpstan-ignore-next-line */
                        \call_user_func($this->userFunction, $entry->value())
                    );
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
