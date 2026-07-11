<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Floe\Exception\ExtensionException;
use Flow\Floe\Exception\FloeException;

use function count;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function sprintf;
use function strlen;
use function substr;

final class FloeValueSerializer
{
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly int $batchSize = 1000,
        private readonly ?bool $useExtension = null,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new FloeException('Serializer batch size must be at least 1');
        }
    }

    public function decode(string $bytes): Row|Rows
    {
        try {
            $footer = $this->readFooter($bytes);

            $filesystem = memory_filesystem();
            $path = path('memory://floe-value.floe');
            $filesystem->writeTo($path)->append($bytes)->close();

            $rows = [];

            foreach ((new FloeReader($filesystem, useExtension: $this->useExtension))
                ->read($path)
                ->recover($this->batchSize) as $batch) {
                foreach ($batch->all() as $row) {
                    $rows[] = $row;
                }
            }

            // recover() salvages a readable prefix; a whole-value decode must not
            // silently return partial data
            if (count($rows) !== $footer->totalRows) {
                throw new FloeException(sprintf(
                    'Floe payload is corrupted, recovered %d of %d rows',
                    count($rows),
                    $footer->totalRows,
                ));
            }

            return RowsValueMapper::reconstructFrom($rows, $footer);
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
    }

    public function encode(Row|Rows $value): string
    {
        $rows = RowsValueMapper::wrap($value);

        $filesystem = memory_filesystem();
        $path = path('memory://floe-value.floe');

        try {
            $writer = new FloeWriter($filesystem, useExtension: $this->useExtension);
            $writer->create($path, RowsValueMapper::metadataFor($value));

            // an empty Rows still crosses once - chunks() would yield nothing and an
            // empty-but-partitioned value would lose its PARTITIONS frame (byte parity)
            foreach ($rows->count() === 0 ? [$rows] : $rows->chunks($this->batchSize) as $chunk) {
                $writer->write($chunk);
            }

            $writer->close();
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }

        return $filesystem->readFrom($path)->content();
    }

    /**
     * @throws FloeException
     */
    private function readFooter(string $bytes): Footer
    {
        $length = strlen($bytes);

        if ($length < (Format::HEADER_LENGTH + Format::TRAILER_LENGTH)) {
            throw new FloeException('Floe payload is torn, too small to hold a header and a trailer');
        }

        $footerLength = Format::parseTrailer(substr($bytes, $length - Format::TRAILER_LENGTH));
        $footerStart = $length - Format::TRAILER_LENGTH - $footerLength;

        if ($footerStart < Format::HEADER_LENGTH) {
            throw new FloeException('Floe payload is torn, footer does not fit inside the payload');
        }

        return Footer::fromJson(substr($bytes, $footerStart, $footerLength));
    }
}
