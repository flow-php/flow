<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;

/**
 * @implements EntryFactory<array<mixed>>
 */
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

    /**
     * @param mixed $value
     *
     * @throws InvalidArgumentException
     * @throws \JsonException
     */
    public function create(string $entryName, mixed $value) : Entry
    {
        if (\is_string($value)) {
            if ($this->isJson($value)) {
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
            if (!\array_is_list($value)) {
                return new Row\Entry\ArrayEntry($entryName, $value);
            }

            $type = null;
            $class = null;

            /** @psalm-suppress MixedAssignment */
            foreach ($value as $valueElement) {
                if ($type === null) {
                    $type = \gettype($valueElement);
                }

                if ($type === 'object' && $class === null) {
                    /** @psalm-suppress MixedArgument */
                    $class = \get_class($valueElement);
                }

                if ($type !== \gettype($valueElement)) {
                    return new Row\Entry\ArrayEntry($entryName, $value);
                }

                /** @psalm-suppress MixedArgument */
                if ($class !== null && $class !== \get_class($valueElement)) {
                    return new Row\Entry\ArrayEntry($entryName, $value);
                }
            }

            if ($class !== null) {
                /**
                 * @psalm-suppress PossiblyNullArgument
                 * @phpstan-ignore-next-line
                 */
                return new Entry\ListEntry($entryName, Entry\TypedCollection\ObjectType::of($class), $value);
            }

            /**
             * @psalm-suppress PossiblyNullArgument
             * @phpstan-ignore-next-line
             */
            return new Entry\ListEntry($entryName, Entry\TypedCollection\ScalarType::fromString($type), $value);
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
        } catch (\Exception) {
            return false;
        }
    }
}
