<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\InvalidArgumentException;

final class ValueConverter
{
    /**
     * @param array<mixed> $avroSchema
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly array $avroSchema)
    {
        try {
            \AvroSchema::real_parse($avroSchema);
        } catch (\AvroSchemaParseException $e) {
            throw new InvalidArgumentException('Avro invalid schema provided', previous: $e);
        }
    }

    /**
     * @param array<mixed> $data
     *
     * @return array<mixed>
     */
    public function convert(array $data) : array
    {
        $convertedData = [];

        foreach ($data as $entry => $value) {
            $avroType = $this->type($entry);

            if ($avroType !== null && \is_array($avroType[\AvroSchema::TYPE_ATTR])) {
                if (($avroType[\AvroSchema::TYPE_ATTR][\AvroSchema::TYPE_ATTR] ?? null) === \AvroSchema::LONG_TYPE
                    && \array_key_exists(\AvroSchema::LOGICAL_TYPE_ATTR, $avroType)
                    && $avroType[\AvroSchema::LOGICAL_TYPE_ATTR] === 'timestamp-micros'
                ) {
                    $convertedData[$entry] = \DateTimeImmutable::createFromFormat(
                        'U.u',
                        \implode('.', \str_split((string) $value, 10))
                    );
                } elseif (($avroType[\AvroSchema::TYPE_ATTR][\AvroSchema::TYPE_ATTR] ?? null) === \AvroSchema::ARRAY_SCHEMA
                    && \array_key_exists(\AvroSchema::LOGICAL_TYPE_ATTR, $avroType[\AvroSchema::TYPE_ATTR])
                    && $avroType[\AvroSchema::TYPE_ATTR][\AvroSchema::LOGICAL_TYPE_ATTR] === 'timestamp-micros'
                ) {
                    $convertedData[$entry] = \array_map(
                        static fn (int $timestamp) : \DateTimeImmutable => \DateTimeImmutable::createFromFormat('U.u', \implode('.', \str_split((string) $timestamp, 10))),
                        $value
                    );
                } else {
                    $convertedData[$entry] = $value;
                }
            } else {
                if ($avroType[\AvroSchema::TYPE_ATTR] === \AvroSchema::LONG_TYPE
                    && \array_key_exists(\AvroSchema::LOGICAL_TYPE_ATTR, $avroType)
                    && $avroType[\AvroSchema::LOGICAL_TYPE_ATTR] === 'timestamp-micros'
                ) {
                    $convertedData[$entry] = \DateTimeImmutable::createFromFormat(
                        'U.u',
                        \implode('.', \str_split((string) $value, 10))
                    );
                } else {
                    $convertedData[$entry] = $value;
                }
            }
        }

        return $convertedData;
    }

    private function type(string $entry) : ?array
    {
        foreach ($this->avroSchema[\AvroSchema::FIELDS_ATTR] as $avroType) {
            if ($avroType[\AvroSchema::NAME_ATTR] === $entry) {
                return $avroType;
            }
        }

        return null;
    }
}
