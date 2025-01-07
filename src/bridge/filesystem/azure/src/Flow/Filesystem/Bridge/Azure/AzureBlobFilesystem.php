<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\{DestinationStream,
    Exception\RuntimeException,
    FileStatus,
    Filesystem,
    Path,
    Protocol,
    SourceStream};

final readonly class AzureBlobFilesystem implements Filesystem
{
    public function __construct(private BlobServiceInterface $blobService, private Options $options)
    {
    }

    public function appendTo(Path $path) : DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->protocol()->validateScheme($path);

        return AzureBlobDestinationStream::openAppend(
            $this->blobService,
            $path,
            $this->options->blockFactory(),
            $this->options->blockSize()
        );
    }

    public function getSystemTmpDir() : Path
    {
        return $this->options->tmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()) : \Generator
    {
        $this->protocol()->validateScheme($path);

        if ($path->isPattern()) {
            $prefix = \ltrim($path->staticPart()->path(), DIRECTORY_SEPARATOR);
        } else {
            $prefix = \ltrim($path->path(), DIRECTORY_SEPARATOR);
        }

        $options = $this->options->listBlobOptions();

        if ($prefix) {
            $options->withPrefix($prefix);
        }

        foreach ($this->blobService->listBlobs($options) as $blob) {
            $blobPath = new Path($path->protocol()->scheme() . DIRECTORY_SEPARATOR . \ltrim($blob->name(), DIRECTORY_SEPARATOR), $path->options());
            $blobFileStatus = new FileStatus($blobPath, (bool) $blobPath->extension());

            if ($path->isPattern() && !$path->matches($blobPath)) {
                continue;
            }

            if ($pathFilter->accept($blobFileStatus)) {
                yield $blobFileStatus;
            }
        }
    }

    public function mv(Path $from, Path $to) : bool
    {
        $this->protocol()->validateScheme($from);
        $this->protocol()->validateScheme($to);

        $this->blobService->copyBlob($from->path(), $to->path());
        $this->blobService->deleteBlob($from->path());

        return true;
    }

    public function protocol() : Protocol
    {
        return new Protocol('azure-blob');
    }

    public function readFrom(Path $path) : SourceStream
    {
        return new AzureBlobSourceStream($path, $this->blobService);
    }

    public function rm(Path $path) : bool
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            return false;
        }

        $this->protocol()->validateScheme($path);

        if ($path->isPattern()) {
            $deletedCount = 0;

            foreach ($this->list($path) as $fileStatus) {
                $this->blobService->deleteBlob($fileStatus->path->path());
                $deletedCount++;
            }

            return (bool) $deletedCount;
        }

        try {
            $this->blobService->deleteBlob($path->path());

            return true;
        } catch (\Exception) {
            /**
             * Since AzureBlobStorage doesn't have a concept of folders, before we check if the intention is not to delete
             * entire path, like for example azure-blob://nested/folder we need to first add / at the end, to accidentally
             * not delete files that would also match the prefix, like: azure-blob://nested/folder_but_file.txt.
             */
            $folderPath = new Path(\trim($path->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR, $path->options());
            $blobProperties = $this->blobService->getBlobProperties($folderPath->path());

            if ($blobProperties === null) {
                $deletedCount = 0;

                foreach ($this->list($folderPath) as $fileStatus) {
                    $this->blobService->deleteBlob($fileStatus->path->path());
                    $deletedCount++;
                }

                return (bool) $deletedCount;
            }

            return false;
        }
    }

    public function status(Path $path) : ?FileStatus
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            return new FileStatus($path, false);
        }

        $this->protocol()->validateScheme($path);

        if (!$path->isPattern()) {
            if ($path->path() === '/') {
                return new FileStatus($path, false);
            }

            $blobProperties = $this->blobService->getBlobProperties(\ltrim($path->path(), DIRECTORY_SEPARATOR));

            if ($blobProperties === null) {
                /**
                 * Since AzureBlobStorage doesn't have a concept of folders, before we check if the intention is not to delete
                 * entire path, like for example azure-blob://nested/folder we need to first add / at the end, to accidentally
                 * not match files that would also match the prefix, like: azure-blob://nested/folder_but_file.txt.
                 */
                $folderPath = new Path(trim($path->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR, $path->options());

                foreach ($this->list($folderPath) as $fileStatus) {
                    return new FileStatus($folderPath, false);
                }

                return null;
            }

            return new FileStatus($path, true);
        }

        foreach ($this->list($path) as $fileStatus) {
            return $fileStatus;
        }

        return null;
    }

    public function writeTo(Path $path) : DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->protocol()->validateScheme($path);

        return AzureBlobDestinationStream::openBlank(
            $this->blobService,
            $path,
            $this->options->blockFactory(),
            $this->options->blockSize()
        );
    }
}
