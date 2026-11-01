<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Split;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\RangeSourceStream;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function min;

use const PHP_INT_MAX;

/**
 * A byte window of one file, read through Extractor::extract() with Criteria::withSplit().
 */
final readonly class FileSplit implements Describable, Split
{
    /**
     * @var null|int<1, max> on-disk bytes the window covers; null means "to the end of the file"
     */
    public ?int $length;

    /**
     * @var null|int<0, max> the listed size of the whole file; a read with it never lists again, null lists as before
     */
    public ?int $size;

    /**
     * @var int<0, max> on-disk byte where the window starts
     */
    public int $start;

    public function __construct(
        public Path $path,
        int $start,
        ?int $length,
        ?int $size = null,
    ) {
        if ($start < 0) {
            throw new InvalidArgumentException('File split start must not be negative, given: ' . $start);
        }

        if ($length !== null && $length < 1) {
            throw new InvalidArgumentException('File split length must be greater than 0, given: ' . $length);
        }

        if ($size !== null && $size < 0) {
            throw new InvalidArgumentException('File split size must not be negative, given: ' . $size);
        }

        $this->start = $start;
        $this->length = $length;
        $this->size = $size;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            Path::fromArray(type_array()->assert($data['path'] ?? null)),
            type_integer()->assert($data['start'] ?? null),
            type_optional(type_integer())->assert($data['length'] ?? null),
            type_optional(type_integer())->assert($data['size'] ?? null),
        );
    }

    /**
     * ceil(size / $targetBytes) windows over $file, the last one shorter; one whole-file split when its size is unknown
     * or 0.
     *
     * @param int<1, max> $targetBytes
     *
     * @return non-empty-list<self>
     */
    public static function windows(SourceFile $file, int $targetBytes): array
    {
        if ($file->size === null || $file->size < 1) {
            return [new self($file->path, 0, null, $file->size)];
        }

        $windows = [];

        for ($start = 0; $start < $file->size; $start += $targetBytes) {
            $windows[] = new self($file->path, $start, min($targetBytes, $file->size - $start), $file->size);
        }

        return $windows;
    }

    /**
     * A unit belongs to the split holding its midpoint; a split without a length owns them all.
     *
     * @param iterable<ByteUnit> $units
     */
    public function owns(iterable $units): RowRange
    {
        $offset = 0;
        $count = 0;

        foreach ($units as $unit) {
            $midpoint = $unit->midpoint();

            if ($this->length !== null && $midpoint >= ($this->start + $this->length)) {
                break;
            }

            if ($midpoint < $this->start) {
                $offset += $unit->rows;

                continue;
            }

            $count += $unit->rows;
        }

        return new RowRange($offset, $count);
    }

    public function bytes(): int
    {
        return $this->length ?? $this->size ?? 0;
    }

    public function describe(): Description
    {
        return new Description(self::class, [
            'path' => $this->path->normalize(),
            'start' => $this->start,
            'length' => $this->length,
            'size' => $this->size,
        ]);
    }

    /**
     * Only the window at the start of the file sees a header, a BOM or any other preamble.
     */
    /**
     * The window [first ? 0 : $start ?? $end, $end ?? size) over $stream; $start and $end are the resynced record
     * boundaries at or after this split's start and end, null when no record starts there.
     *
     * @param null|int<0, max> $start
     * @param null|int<0, max> $end
     */
    public function window(SourceStream $stream, ?int $start, ?int $end): RangeSourceStream
    {
        $end ??= $stream->size() ?? PHP_INT_MAX;

        return new RangeSourceStream($stream, $this->isFirst() ? 0 : $start ?? $end, $end);
    }

    public function isFirst(): bool
    {
        return $this->start === 0;
    }
}
