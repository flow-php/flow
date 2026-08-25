<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function sprintf;

final class FilesExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. files($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
    }

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $fileStatus) {
            $signal = yield array_to_rows([
                'path' => $fileStatus->path->path(),
                'protocol' => $fileStatus->path->protocol(),
                'file_name' => $fileStatus->path->filename(),
                'base_name' => $fileStatus->path->basename(),
                'is_file' => $fileStatus->isFile(),
                'is_dir' => $fileStatus->isDirectory(),
                'extension' => $fileStatus->path->extension(),
            ], $context->hydrator());

            $this->incrementReturnedRows();

            if ($signal === Signal::STOP || $this->reachedLimit()) {
                return;
            }
        }
    }

    public function source(): Path
    {
        return $this->path;
    }
}
