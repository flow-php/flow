<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\TypedCollection\ObjectType;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Row\Entry\TypedCollection\Type;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Row\ValueConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToBooleanCaster;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToFloatCaster;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToIntegerCaster;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToStringCaster;
use Flow\ETL\Transformer\Cast\ValueCaster\StringToDateTimeCaster;

/**
 * @implements EntryConverter<array{type: Type}>
 * @psalm-immutable
 */
final class AnyToListCaster implements EntryConverter
{
    public function __construct(private readonly Type $type, private readonly ?ValueConverter $valueConverter = null)
    {
    }

    public function __serialize() : array
    {
        return ['type' => $this->type];
    }

    public function __unserialize(array $data) : void
    {
        $this->type = $data['type'];
    }

    public function convert(Entry $entry) : Entry
    {
        /**
         * @psalm-suppress ImpureFunctionCall
         */
        return new Entry\ListEntry(
            $entry->name(),
            $this->type,
            \array_map(
                function (mixed $value) : mixed {
                    if ($this->valueConverter !== null) {
                        return $this->valueConverter->convert($value);
                    }

                    if ($this->type instanceof ObjectType) {
                        if (\is_a($this->type->class, \DateTimeInterface::class, true) && \is_string($value)) {
                            return (new StringToDateTimeCaster())->convert($value);
                        }

                        throw new RuntimeException('Value ' . \gettype($value) . " can't be automatically cast {$this->type->toString()}, please provide custom ValueConverter.");
                    }

                    /** @var ScalarType $type */
                    $type = $this->type;

                    return match ($type) {
                        ScalarType::integer => (new AnyToIntegerCaster())->convert($value),
                        ScalarType::string => (new AnyToStringCaster())->convert($value),
                        ScalarType::boolean => (new AnyToBooleanCaster())->convert($value),
                        ScalarType::float => (new AnyToFloatCaster())->convert($value),
                    };
                },
                (array) $entry->value()
            )
        );
    }
}
