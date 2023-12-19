<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Filesystem;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;

final class RemoteFileListExtractor implements Extractor, Extractor\FileExtractor, Extractor\LimitableExtractor
{
    use Limitable;

    public function __construct(
        private readonly Path $directory,
        private readonly bool $recursive,
        private readonly FlysystemFactory $factory = new FlysystemFactory()
    ) {
        if ($this->directory->isLocal()) {
            throw new InvalidArgumentException('Path must point to a remote directory, got local path ' . $this->directory->uri() . ' instead');
        }

        if ($this->directory->isPattern()) {
            throw new InvalidArgumentException('RemoteFileListExtractor does not support glob paths, got ' . $this->directory->path());
        }
    }

    public function extract(FlowContext $context) : \Generator
    {
        $fs = $this->factory->create($this->directory);

        /** @var DirectoryAttributes|FileAttributes $file */
        foreach ($fs->listContents($this->directory->path(), $this->recursive) as $file) {
            $path = new Path($this->directory->scheme() . '://' . $file->path(), $this->directory->options());

            /**
             * @psalm-suppress PossiblyUndefinedMethod
             */
            $signal = yield array_to_rows([
                'path' => $file->path(),
                'uri' => $path->uri(),
                'scheme' => $path->scheme(),
                'base_name' => $path->basename(),
                'file_name' => $path->filename(),
                'extension' => $path->extension(),
                'is_file' => $file->isFile(),
                'is_dir' => $file->isDir(),
                /** @phpstan-ignore-next-line */
                'size' => $file->isFile() ? $file->fileSize() : null,
                'visibility' => $file->visibility(),
                'metadata' => $file->extraMetadata(),
                /** @phpstan-ignore-next-line */
                'mime_type' => $file->isFile() ? $file->mimeType() : null,
                'last_modified' => $file->lastModified(),
            ]);

            $this->countRow();

            if ($signal === Signal::STOP || $this->reachedLimit()) {
                return;
            }
        }
    }

    public function source() : Path
    {
        return $this->directory;
    }
}
