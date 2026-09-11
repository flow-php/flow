<?php

declare(strict_types=1);

namespace Flow\CLI\Arguments;

use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

use function Flow\CLI\argument_string;
use function Flow\Filesystem\DSL\path_real;
use function sprintf;

final readonly class FilePathArgument
{
    public function __construct(
        private string $path,
        private Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {}

    public function get(InputInterface $input): Path
    {
        $path = path_real(argument_string($this->path, $input));

        if (!$this->filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'The Flow CLI reads and writes "file://" paths only, given: "%s". Build the filesystem '
                . 'in a pipeline file and run it with `flow run <pipeline.php>` - e.g. '
                . "to_parquet(path('aws-s3://...'), filesystem: aws_s3_filesystem(...)).",
                $path->uri(),
            ));
        }

        return $path;
    }

    public function getExisting(InputInterface $input): Path
    {
        $path = path_real(argument_string($this->path, $input));

        if (!$this->filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'The Flow CLI reads and writes "file://" paths only, given: "%s". Build the filesystem '
                . 'in a pipeline file and run it with `flow run <pipeline.php>` - e.g. '
                . "to_parquet(path('aws-s3://...'), filesystem: aws_s3_filesystem(...)).",
                $path->uri(),
            ));
        }

        if ($this->filesystem->status($path) === null) {
            throw new InvalidArgumentException("File '{$path->path()}' does not exist.");
        }

        return $path;
    }

    public function getNotExisting(InputInterface $input): Path
    {
        $path = path_real(argument_string($this->path, $input));

        if (!$this->filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'The Flow CLI reads and writes "file://" paths only, given: "%s". Build the filesystem '
                . 'in a pipeline file and run it with `flow run <pipeline.php>` - e.g. '
                . "to_parquet(path('aws-s3://...'), filesystem: aws_s3_filesystem(...)).",
                $path->uri(),
            ));
        }

        if ($this->filesystem->status($path) !== null) {
            throw new InvalidArgumentException("File '{$path->path()}' already exist.");
        }

        return $path;
    }
}
