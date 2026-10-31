<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\CorruptedRecordsRouter;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Reader;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;

final readonly class FileSplitReader implements Describable, Reader, CorruptedRecordsRouter
{
    public function __construct(
        private FileRead $read,
        private FileBatches $batches,
        private ?Loader $corruptedRecordsLoader = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            FileRead::fromArray(type_array()->assert($data['read'] ?? null), $rebuild),
            $rebuild->of($data['batches'] ?? null, FileBatches::class),
            ($data['corrupted_records_loader'] ?? null) === null
                ? null
                : $rebuild->of($data['corrupted_records_loader'], Loader::class),
        );
    }

    public function corruptedRecordsLoader(): ?Loader
    {
        return $this->corruptedRecordsLoader;
    }

    /**
     * Null when the reject loader does not describe: such a read runs only in this process.
     */
    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'read' => $this->read->normalize(),
            'batches' => $this->batches,
            'corrupted_records_loader' => $this->corruptedRecordsLoader,
        ]);
    }

    /**
     * @return Generator<int, CorruptedRecordException|Rows, null|Signal, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        try {
            yield from $this->read->scan($backend, $criteria, $this->batches);
        } finally {
            $this->batches->close();
        }
    }

    public function withCorruptedRecords(?Loader $loader): static
    {
        return new self($this->read, $this->batches, $loader);
    }
}
