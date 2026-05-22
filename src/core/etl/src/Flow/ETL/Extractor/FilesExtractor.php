<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\Filesystem\Path;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class FilesExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    public function __construct(
        private readonly Path $path,
    ) {}

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($context->filesystem($this->path)->list($this->path, $this->filter()) as $fileStatus) {
            $signal = yield array_to_rows([
                'path' => $fileStatus->path->path(),
                'protocol' => $fileStatus->path->protocol(),
                'file_name' => $fileStatus->path->filename(),
                'base_name' => $fileStatus->path->basename(),
                'is_file' => $fileStatus->isFile(),
                'is_dir' => $fileStatus->isDirectory(),
                'extension' => $fileStatus->path->extension(),
            ], $context->entryFactory());

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
