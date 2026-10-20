<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\Filesystem\FileStatus;

final readonly class FileMetadataRow implements ListedFileRow
{
    public function row(FileStatus $status): array
    {
        $extension = $status->path->extension();

        return [
            'path' => $status->path->path(),
            'protocol' => $status->path->protocol(),
            'file_name' => $status->path->filename(),
            'base_name' => $status->path->basename(),
            'is_file' => $status->isFile(),
            'is_dir' => $status->isDirectory(),
            // Path::extension() answers false for an extensionless file; the column is one
            // type, so the absence is spelled null rather than a boolean in a string column.
            'extension' => $extension === false ? null : $extension,
        ];
    }
}
