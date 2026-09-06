<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use JsonException;

use function array_key_exists;
use function json_encode;
use function ord;
use function sprintf;
use function strlen;

use const JSON_THROW_ON_ERROR;

/**
 * @implements Encoder<string>
 */
final class PhpFloeEncoder implements Encoder
{
    /**
     * @var null|array<int, ColumnBlueprint>
     */
    private ?array $decodePlan = null;

    /**
     * @var null|array<string, Encoding\ValueEncoder> per-column value encoder, keyed by column name
     */
    private ?array $encoders = null;

    private ?string $schemaBody = null;

    private readonly SchemaDecoder $schemaDecoder;

    public function __construct(
        private readonly Schema $schema,
    ) {
        $this->schemaDecoder = new SchemaDecoder(new ValueDecoder());
    }

    public function decode(array $batch): array
    {
        $decodePlan = $this->decodePlan ??= $this->schemaDecoder->decode($this->schemaBody());

        $decoded = [];

        foreach ($batch as $body) {
            $position = 0;
            $values = [];
            $metadata = [];

            foreach ($decodePlan as $column) {
                $flag = ord($body[$position++]);

                if ($flag === Format::VALUE_PRESENT) {
                    $values[$column->name] = $column->decoder->decode($body, $position);
                } elseif ($flag === Format::VALUE_NULL) {
                    $values[$column->name] = null;
                } elseif ($flag === Format::VALUE_PRESENT_WITH_META) {
                    $metadata[$column->name] = Format::readMetadata($body, $position);
                    $values[$column->name] = $column->decoder->decode($body, $position);
                } elseif ($flag === Format::VALUE_NULL_WITH_META) {
                    $metadata[$column->name] = Format::readMetadata($body, $position);
                    $values[$column->name] = null;
                } else {
                    throw new FloeException(sprintf('Floe found unknown value flag 0x%02X', $flag));
                }
            }

            if ($position !== strlen($body)) {
                throw new FloeException('Floe row frame length does not match its content');
            }

            $decoded[] = new RawRowValues($values, $metadata);
        }

        return $decoded;
    }

    public function encode(array $batch): array
    {
        $encoders = $this->encoders ??= $this->buildEncoders();

        $bodies = [];

        foreach ($batch as $rowIndex => $rowValues) {
            $body = '';

            foreach ($this->schema->definitions() as $name => $definition) {
                if (!array_key_exists($name, $rowValues->values)) {
                    throw new FloeException(sprintf(
                        'Floe found a row that does not carry the declared column "%s"',
                        $name,
                    ));
                }

                // @mago-ignore analysis:mixed-assignment
                $value = $rowValues->values[$name];
                $metadata = $rowValues->metadata[$name] ?? Metadata::empty();
                $columnMetadata = $definition->metadata();
                $diverges = $metadata->isEmpty() && $columnMetadata->isEmpty()
                    ? false
                    : !$metadata->isEqual($columnMetadata);

                if ($value === null) {
                    if (!$definition->isNullable()) {
                        throw new SchemaMismatchException($rowIndex, ColumnMismatchException::valueDoesNotMatch(
                            $definition,
                            null,
                        ));
                    }

                    $body .= $diverges
                        ? Format::VALUE_NULL_WITH_META_BYTE . Format::metadataBytes($metadata)
                        : Format::VALUE_NULL_BYTE;

                    continue;
                }

                $body .= $diverges
                    ? Format::VALUE_PRESENT_WITH_META_BYTE
                    . Format::metadataBytes($metadata)
                    . $encoders[$name]->encode($value)
                    : Format::VALUE_PRESENT_BYTE . $encoders[$name]->encode($value);
            }

            $bodies[] = $body;
        }

        return $bodies;
    }

    /**
     * @return array<string, Encoding\ValueEncoder>
     */
    private function buildEncoders(): array
    {
        $valueEncoder = new ValueEncoder();
        $encoders = [];

        foreach ($this->schema->definitions() as $name => $definition) {
            $encoders[$name] = $valueEncoder->encoderFor($definition);
        }

        return $encoders;
    }

    private function schemaBody(): string
    {
        if ($this->schemaBody !== null) {
            return $this->schemaBody;
        }

        try {
            return $this->schemaBody = json_encode($this->schema->normalize(), JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new FloeException('Floe failed to encode schema as JSON: ' . $e->getMessage(), 0, $e);
        }
    }
}
