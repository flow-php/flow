<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\{Extractor, FlowContext};

final class LocalFileListExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;

    public function __construct(
        private readonly Path $directory,
        private readonly bool $recursive = false
    ) {
        if (!$this->directory->isLocal()) {
            throw new InvalidArgumentException('Path must point to a local directory, got ' . $this->directory->uri() . ' instead');
        }

        if ($this->directory->isPattern()) {
            throw new InvalidArgumentException('LocalFileListExtractor does not support glob paths, got ' . $this->directory->path());
        }
    }

    public function extract(FlowContext $context) : \Generator
    {
        if ($this->recursive) {
            $files = new \RecursiveIteratorIterator(
                new \RecursiveDirectoryIterator(
                    $this->directory->path(),
                    \RecursiveDirectoryIterator::SKIP_DOTS
                ),
                \RecursiveIteratorIterator::SELF_FIRST
            );
        } else {
            $files = new \DirectoryIterator($this->directory->path());
        }

        /** @var \SplFileInfo $file */
        foreach ($files as $file) {
            $signal = yield array_to_rows([
                'path' => $file->getPath(),
                'real_path' => $file->getRealPath(),
                'path_name' => $file->getPathname(),
                'file_name' => $file->getFilename(),
                'base_name' => $file->getBasename(),
                'is_file' => $file->isFile(),
                'is_dir' => $file->isDir(),
                'is_link' => $file->isLink(),
                'is_executable' => $file->isExecutable(),
                'is_readable' => $file->isReadable(),
                'is_writable' => $file->isWritable(),
                'link_target' => $file->isLink() ? $file->getLinkTarget() : null,
                'owner' => $file->getOwner(),
                'group' => $file->getGroup(),
                'permissions' => $file->getPerms(),
                'inode' => $file->getInode(),
                'file_type' => $file->getType(),
                'extension' => $file->getExtension(),
                'size' => $file->getSize(),
                'last_accessed' => $file->getATime(),
                'last_inode_change_time' => $file->getCTime(),
                'last_modified' => $file->getMTime(),
            ], $context->entryFactory());

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
