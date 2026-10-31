<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Extractor;

use Flow\ETL\Adapter\Excel\IO\Extractor\Sheet\OpenSheet;
use Flow\Filesystem\Path;
use Generator;

final class WorkbookSheet
{
    /**
     * @var array<int, array<array-key, mixed>> raw cells by their row number
     */
    private array $buffered = [];

    private bool $headerResolved = false;

    private ?OpenSheet $sheet = null;

    public function __construct(
        private readonly Path $path,
        private readonly WorkbookReader $reader,
    ) {}

    public function close(): void
    {
        $this->sheet?->close();
        $this->sheet = null;
        $this->headerResolved = false;
        $this->buffered = [];
    }

    /**
     * @return list<string>
     */
    public function columns(): array
    {
        $sheet = $this->sheet ??= $this->reader->open($this->path);

        if (!$this->headerResolved) {
            $this->headerResolved = true;
            $cells = $sheet->cells->current();

            if ($cells !== null) {
                $number = $sheet->cells->key();
                $sheet->cells->next();

                // a sheet without a header row sizes its names by its first row, which stays a row
                if ($sheet->decoder->decode($cells) !== null) {
                    $this->buffered[$number] = $cells;
                }
            }
        }

        return $sheet->decoder->headers() ?? [];
    }

    /**
     * @param array<array-key, mixed> $cells a row rows() or sample() yielded
     *
     * @return null|array<string, mixed>|string see RecordDecoder::decode()
     */
    public function decode(array $cells): array|string|null
    {
        $sheet = $this->sheet ??= $this->reader->open($this->path);

        return $sheet->decoder->decode($cells);
    }

    /**
     * rows() for a bounded schema sample: every row it hands out stays buffered and the sheet stays open, so the
     * rows() that follows replays the sample and parses on from where the sample stopped, instead of parsing the
     * sample a second time. Closing the sheet stays with its owner.
     *
     * @return Generator<int, array<array-key, mixed>> raw cells by their row number
     */
    public function sample(): Generator
    {
        $sheet = $this->sheet ??= $this->reader->open($this->path);
        $this->columns();

        foreach ($this->buffered as $number => $cells) {
            yield $number => $cells;
        }

        while (($cells = $sheet->cells->current()) !== null) {
            $number = $sheet->cells->key();
            // advanced before the row is handed out: a sampler that stops here leaves the cursor past everything
            // buffered, so rows() neither repeats nor skips a row
            $sheet->cells->next();
            $this->buffered[$number] = $cells;

            yield $number => $cells;
        }
    }

    /**
     * @return Generator<int, array<array-key, mixed>> raw cells by their row number, the header row excluded
     */
    public function rows(): Generator
    {
        // the handle THIS generator reads is captured up front and closed by name: an abandoned generator is
        // destroyed whenever the collector gets to it, and it must never close a handle re-opened since
        $sheet = $this->sheet ??= $this->reader->open($this->path);

        try {
            $this->columns();

            foreach ($this->buffered as $number => $cells) {
                yield $number => $cells;
            }

            $this->buffered = [];

            while (($cells = $sheet->cells->current()) !== null) {
                yield $sheet->cells->key() => $cells;

                $sheet->cells->next();
            }
        } finally {
            if ($this->sheet === $sheet) {
                $this->close();
            } else {
                $sheet->close();
            }
        }
    }
}
