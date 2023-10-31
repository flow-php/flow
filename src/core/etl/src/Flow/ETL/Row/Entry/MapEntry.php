<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\Schema\FlowMetadata;
use Flow\ETL\Row\Schema\Metadata;

/**
 * @implements Entry<array<array-key, mixed>, array{name: string, map: array<array-key, mixed>, metadata: array{key_type: string, value_type: string}}>
 */
final class MapEntry implements \Stringable, Entry
{
    use EntryRef;

    /**
     * @var array{key_type: string, value_type: string}
     */
    private array $metadata = [
        'key_type' => 'null',
        'value_type' => 'null',
    ];

    /**
     * @param array<array-key, mixed> $map
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly array $map
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $keysAsString = false;
        $keysAsInteger = false;
        $valuesType = 'null';

        $getValueType = function (mixed $value) : string {
            if (\is_object($value)) {
                return 'object<' . $value::class . '>';
            }

            return \gettype($value);
        };

        foreach ($map as $key => $value) {
            if (!$keysAsString && \is_string($key)) {
                $keysAsString = true;
            }

            if (!$keysAsInteger && \is_int($key)) {
                $keysAsInteger = true;
            }

            if ($keysAsString && $keysAsInteger) {
                throw InvalidArgumentException::because('All keys in map must have the same type.');
            }

            if ('null' === $valuesType) {
                $valuesType = $getValueType($value);
            } elseif ($valuesType !== $getValueType($value)) {
                throw InvalidArgumentException::because('All values in map must have the same type.');
            }
        }

        $this->metadata['key_type'] = $keysAsString ? 'string' : 'int';
        $this->metadata['value_type'] = $valuesType;
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'map' => $this->map, 'metadata' => $this->metadata];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->map = $data['map'];
        $this->metadata = $data['metadata'];
    }

    public function definition() : Definition
    {
        return Definition::map_entry(
            $this->name,
            metadata: Metadata::with(FlowMetadata::METADATA_MAP_KEY_TYPE, $this->metadata['key_type'])
                ->add(FlowMetadata::METADATA_MAP_VALUE_TYPE, $this->metadata['value_type'])
        );
    }

    public function is(string|Reference $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name())
            && $entry instanceof self
            && $this->metadata === $entry->metadata()
            && (new ArrayComparison())->equals($this->map, $entry->value());
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->map));
    }

    public function metadata() : array
    {
        return $this->metadata;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->map);
    }

    public function toString() : string
    {
        return \json_encode($this->map, \JSON_THROW_ON_ERROR);
    }

    public function value() : array
    {
        return $this->map;
    }
}
