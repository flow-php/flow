<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;

final class NativeEntryFactory implements EntryFactory
{
    private const JSON_DEPTH = 512;

    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function create(string $entryName, $value) : Entry
    {
        if (\is_string($value)) {
            if (\class_exists('\\Flow\\ETL\\Row\\Entry\\JsonEntry') && $this->isJson($value)) {
                /** @psalm-suppress MixedArgument */
                return new Row\Entry\JsonEntry($entryName, (array) \json_decode($value, true, self::JSON_DEPTH, JSON_THROW_ON_ERROR));
            }

            return new Row\Entry\StringEntry($entryName, $value);
        }

        if (\is_float($value)) {
            return new Row\Entry\FloatEntry($entryName, $value);
        }

        if (\is_int($value)) {
            return new Row\Entry\IntegerEntry($entryName, $value);
        }

        if (\is_bool($value)) {
            return new Row\Entry\BooleanEntry($entryName, $value);
        }

        if (\is_object($value)) {
            if ($value instanceof \DateTimeImmutable) {
                return new Row\Entry\DateTimeEntry($entryName, $value);
            }

            return new Row\Entry\ObjectEntry($entryName, $value);
        }

        if (\is_array($value)) {
            return new Row\Entry\ArrayEntry($entryName, $value);
        }

        if (null === $value) {
            return new Row\Entry\NullEntry($entryName);
        }

        $type = \gettype($value);

        throw new InvalidArgumentException("{$type} can't be converted to any known Entry");
    }

    private function isJson(string $string) : bool
    {
        try {
            /**
             * @psalm-suppress UnusedFunctionCall
             *
             * @var mixed $value
             */
            $value = \json_decode($string, true, self::JSON_DEPTH, JSON_THROW_ON_ERROR);

            return \is_array($value);
        } catch (\Exception $e) {
            return false;
        }
    }
}
