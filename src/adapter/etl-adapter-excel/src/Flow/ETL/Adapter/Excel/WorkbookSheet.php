<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Adapter\Excel\Sheet\OpenSheet;
use Flow\ETL\Row\RawRowValues;
use Flow\Filesystem\Path;
use Generator;

final class WorkbookSheet
{
    /**
     * @var list<RawRowValues>
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
            $row = $sheet->cells->current();

            if ($row !== null) {
                $this->buffered = $sheet->encoder->decode([$row]);
                $sheet->cells->next();
            }
        }

        return $sheet->encoder->headers() ?? [];
    }

    /**
     * @return Generator<int, RawRowValues>
     */
    public function rows(): Generator
    {
        // the handle THIS generator reads is captured up front and closed by name: an abandoned generator is
        // destroyed whenever the collector gets to it, and it must never close a handle re-opened since
        $sheet = $this->sheet ??= $this->reader->open($this->path);

        try {
            $this->columns();

            foreach ($this->buffered as $rowValues) {
                yield $rowValues;
            }

            $this->buffered = [];

            while (($row = $sheet->cells->current()) !== null) {
                foreach ($sheet->encoder->decode([$row]) as $rowValues) {
                    yield $rowValues;
                }

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
