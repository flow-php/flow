<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Reader;
use Flow\ETL\Values\Column\Backend;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;

final readonly class FileSplitReader implements Describable, Reader
{
    public function __construct(
        private FileRead $read,
        private FileBatches $batches,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            FileRead::fromArray(type_array()->assert($data['read'] ?? null), $rebuild),
            $rebuild->of($data['batches'] ?? null, FileBatches::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'read' => $this->read->normalize(),
            'batches' => $this->batches,
        ]);
    }

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        try {
            yield from $this->read->scan($backend, $criteria, $this->batches);
        } finally {
            $this->batches->close();
        }
    }
}
