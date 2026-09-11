<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use ZipArchive;

use function str_starts_with;

final readonly class ExcelFormatDetector
{
    public function __construct(
        private Filesystem $filesystem,
    ) {}

    public function detect(Path $path): ExcelReader
    {
        $byExtension = ExcelReader::tryFrom((string) $path->extension());

        if ($byExtension !== null) {
            return $byExtension;
        }

        $stream = $this->filesystem->readFrom($path);

        try {
            $head = $stream->read(8, 0);
        } finally {
            $stream->close();
        }

        // the legacy XLS compound-file signature; handed to the XLSX reader, which reports the failure itself
        if (str_starts_with($head, "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1")) {
            return ExcelReader::XLSX;
        }

        if (str_starts_with($head, "\x50\x4B\x03\x04")) {
            $zip = new ZipArchive();

            if ($zip->open($path->path()) === true) {
                $mimetype = $zip->getFromName('mimetype');
                $zip->close();

                return $mimetype === 'application/vnd.oasis.opendocument.spreadsheet'
                    ? ExcelReader::ODS
                    : ExcelReader::XLSX;
            }
        }

        throw new InvalidArgumentException('Unsupported file format: ' . ($path->extension() ?: 'n/a'));
    }
}
