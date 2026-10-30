<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
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

    public function schema(): Schema
    {
        return new Schema(
            new StringDefinition('path'),
            new StringDefinition('protocol'),
            new StringDefinition('file_name'),
            new StringDefinition('base_name'),
            new BooleanDefinition('is_file'),
            new BooleanDefinition('is_dir'),
            new StringDefinition('extension', nullable: true),
        );
    }
}
