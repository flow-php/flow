<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\S3\Exception\NoSuchKeyException;
use AsyncAws\S3\S3Client;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\{DestinationStream, FileStatus, Filesystem, Path, Protocol, SourceStream};

final class AsyncAWSS3Filesystem implements Filesystem
{
    public function __construct(private readonly S3Client $s3Client, private readonly Options $options)
    {

    }

    public function appendTo(Path $path) : DestinationStream
    {
    }

    public function getSystemTmpDir() : Path
    {
        return $this->options->tmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()) : \Generator
    {
        // TODO: Implement list() method.
    }

    public function mv(Path $from, Path $to) : bool
    {
        // TODO: Implement mv() method.
    }

    public function protocol() : Protocol
    {
        return new Protocol('aws-s3');
    }

    public function readFrom(Path $path) : SourceStream
    {
        return new S3SourceStream($path, $this->s3Client);
    }

    public function rm(Path $path) : bool
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            return false;
        }

        try {
            $this->s3Client->deleteObject([
                'Bucket' => $path->rootDirectoryName(),
                'Key' => $path->skipDirectories(1)?->path(),
            ]);

            return true;
        } catch (NoSuchKeyException $e) {
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

            try {
                $headObject = $this->s3Client->headObject([
                    'Bucket' => $path->rootDirectoryName(),
                    'Key' => $path->skipDirectories(1)?->path(),
                ]);
                $headObject->resolve();

                return new FileStatus($path, true);
            } catch (NoSuchKeyException $e) {
                /**
                 * Since S3 doesn't have a concept of folders, before we check if the intention is not to delete
                 * entire path, like for example aws-s3://nested/folder we need to first add / at the end, to accidentally
                 * not match files that would also match the prefix, like: aws-s3://nested/folder_but_file.txt.
                 */
                $folderPath = new Path(trim($path->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR, $path->options());

                foreach ($this->list($folderPath) as $fileStatus) {
                    return new FileStatus($folderPath, false);
                }

                return null;
            }
        }

        foreach ($this->list($path) as $fileStatus) {
            return $fileStatus;
        }

        return null;
    }

    public function writeTo(Path $path) : DestinationStream
    {
        return S3DestinationStream::openBlank($this->s3Client, $path, $this->options->blockFactory(), $this->options->partSize());
    }
}
