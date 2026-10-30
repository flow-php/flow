<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\IO\Loader;

use CmsIg\Seal\EngineInterface;
use DateTimeInterface;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Generator;
use SensitiveParameter;

use function array_key_exists;
use function is_int;
use function is_string;

final class SealLoader implements Loader
{
    private int $bulkSize = 100;

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private ?SealEncoder $encoder = null;

    private string $identifierColumn = 'id';

    public function __construct(
        private readonly EngineInterface $engine,
        private readonly string $index,
        private readonly Operation $operation = Operation::UPSERT,
    ) {}

    public function load(Rows $rows, Backend $backend): void
    {
        if (!$rows->count()) {
            return;
        }

        $this->engine->bulk(
            $this->index,
            $this->operation === Operation::UPSERT ? $this->encoder()->encode($rows) : [],
            $this->operation === Operation::DELETE ? $this->deleteIdentifiers($rows) : [],
            $this->bulkSize,
        );
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function withBulkSize(int $bulkSize): self
    {
        $this->bulkSize = $bulkSize;

        return $this;
    }

    public function withDateFormat(string $dateFormat): self
    {
        $this->dateFormat = $dateFormat;

        return $this;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withIdentifierColumn(string $column): self
    {
        $this->identifierColumn = $column;

        return $this;
    }

    private function encoder(): SealEncoder
    {
        return $this->encoder ??= new SealEncoder($this->dateTimeFormat, $this->dateFormat);
    }

    /**
     * @return Generator<int, string>
     */
    private function deleteIdentifiers(Rows $rows): Generator
    {
        foreach ($this->encoder()->encode($rows) as $document) {
            // @mago-ignore analysis:mixed-assignment
            $identifier = array_key_exists($this->identifierColumn, $document)
                ? $document[$this->identifierColumn]
                : null;

            if (!is_string($identifier) && !is_int($identifier)) {
                throw new RuntimeException(
                    'Column "'
                    . $this->identifierColumn
                    . '" cannot be used as a document identifier for DELETE operation',
                );
            }

            yield (string) $identifier;
        }
    }
}
