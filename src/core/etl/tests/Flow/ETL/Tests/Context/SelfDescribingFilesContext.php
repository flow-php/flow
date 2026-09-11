<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Extractor\SelfDescribingFile;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Double\FakeSelfDescribingFile;
use Generator;

use function array_values;
use function Flow\Filesystem\DSL\path;

final class SelfDescribingFilesContext
{
    public int $advanced = 0;

    /**
     * @var list<FakeSelfDescribingFile>
     */
    public readonly array $files;

    public function __construct(FakeSelfDescribingFile ...$files)
    {
        // a variadic is array<array-key, T> to the analyzer, not a list
        $this->files = array_values($files);
    }

    public static function describing(Schema ...$schemas): self
    {
        $files = [];
        $index = 0;

        foreach ($schemas as $schema) {
            $files[] = new FakeSelfDescribingFile($schema, self::source($index));
            $index++;
        }

        return new self(...$files);
    }

    public static function failing(): self
    {
        return new self(new FakeSelfDescribingFile(new Schema(), self::source(0), describingThrows: true));
    }

    /**
     * @return list<FakeSelfDescribingFile>
     */
    public function closed(): array
    {
        $closed = [];

        foreach ($this->files as $file) {
            if ($file->closed) {
                $closed[] = $file;
            }
        }

        return $closed;
    }

    /**
     * @return Generator<int, SelfDescribingFile>
     */
    public function generator(): Generator
    {
        foreach ($this->files as $file) {
            $this->advanced++;

            yield $file;
        }
    }

    private static function source(int $index): SourceFile
    {
        return new SourceFile(path('memory://orders/file' . $index . '.csv'));
    }
}
