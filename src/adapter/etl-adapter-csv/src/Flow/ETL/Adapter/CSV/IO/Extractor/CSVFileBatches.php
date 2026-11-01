<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaValidationException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileBatches;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;

final readonly class CSVFileBatches implements FileBatches
{
    /**
     * @param null|InferredColumns $inferred the columns every file must carry; null when declared or union by name
     */
    /**
     * @param null|SchemaInference $inference the inference that made the schema; null when it was declared
     */
    public function __construct(
        private CSVSourceOpener $opener,
        private ?InferredColumns $inferred,
        private ?SchemaInference $inference = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $inferred = type_optional(type_array())->assert($data['inferred'] ?? null);
        $inference = type_optional(type_array())->assert($data['inference'] ?? null);

        return new self(
            $rebuild->of($data['opener'] ?? null, CSVSourceOpener::class),
            $inferred === null ? null : InferredColumns::fromArray($inferred),
            $inference === null ? null : SchemaInference::fromArray($inference),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'opener' => $this->opener,
            'inferred' => $this->inferred?->normalize(),
            'inference' => $this->inference?->normalize(),
        ]);
    }

    /**
     * The columns of every file must be the ones the inferred schema was derived from - the opener checks them before
     * the file's first batch is built, so a cell the inferred types refuse cannot hide a file whose columns diverge.
     * A split reads only its byte window of the file.
     *
     * @throws InferredSchemaException|SchemaValidationException
     *
     * @return Generator<int, CorruptedRecordException|Rows, mixed, int>
     */
    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $open = $criteria->split === null
            ? $this->opener->open($source, $this->inferred)
            : $this->opener->openSplit($source, $criteria->split, $this->inferred);

        try {
            // the header is resolved by the time the first batch or corrupted record is handed out
            $checked = $this->inference !== null;

            foreach ($open->batches($body, $read->batchSize, $backend, $this->inference) as $batch) {
                if (!$checked) {
                    $checked = true;
                    $missing = $body->missingFrom($open->headers());

                    if ($missing !== []) {
                        throw SchemaValidationException::declaredColumnsMissing(
                            $source->uri(),
                            $missing,
                            $open->headers(),
                        );
                    }
                }

                yield $batch;
            }
        } finally {
            $open->close();
        }

        return 0;
    }

    public function close(): void {}
}
