<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Filesystem\Path\{Options, UnixPath, WindowsPath};
use Flow\Filesystem\Stream\ResourceContext;

final class Path
{
    private WindowsPath|UnixPath $implementation;

    /**
     * @param array<string, mixed>|Options $options
     */
    public function __construct(string $uri, array|Options $options = [])
    {
        $this->implementation = \PHP_OS_FAMILY === 'Windows'
            ? new WindowsPath($uri, $options)
            : new UnixPath($uri, $options);
    }

    /**
     * Turn relative path into absolute paths even when path does not exists or it's glob pattern.
     *
     * @param array<string, mixed>|Options $options
     *
     * @throws InvalidArgumentException
     * @throws RuntimeException
     */
    public static function realpath(string $path, array|Options $options = []) : self
    {
        $instance = new self('', $options);
        $instance->implementation = \PHP_OS_FAMILY === 'Windows'
            ? WindowsPath::realpath($path, $options)
            : UnixPath::realpath($path, $options);

        return $instance;
    }

    public function addPartitions(Partition $partition, Partition ...$partitions) : self
    {
        return $this->createFromImplementation($this->implementation->addPartitions($partition, ...$partitions));
    }

    public function basename() : string
    {
        return $this->implementation->basename();
    }

    public function basenamePrefix(string $prefix) : self
    {
        return $this->createFromImplementation($this->implementation->basenamePrefix($prefix));
    }

    public function context() : ResourceContext
    {
        return ResourceContext::from($this);
    }

    public function endsWith(string $string) : bool
    {
        return $this->implementation->endsWith($string);
    }

    public function extension() : string|false
    {
        return $this->implementation->extension();
    }

    public function filename() : string
    {
        return $this->implementation->filename();
    }

    public function isEqual(self $path) : bool
    {
        return $this->implementation->isEqual($path->implementation);
    }

    public function isLocal() : bool
    {
        return $this->implementation->protocol()->is('file');
    }

    public function isPattern() : bool
    {
        return $this->implementation->isPattern();
    }

    public function matches(self $path) : bool
    {
        return $this->implementation->matches($path->implementation);
    }

    public function options() : Options
    {
        return $this->implementation->options();
    }

    public function parentDirectory() : self
    {
        return $this->createFromImplementation($this->implementation->parentDirectory());
    }

    public function partitions() : Partitions
    {
        return $this->implementation->partitions();
    }

    /**
     * @return array<Path>
     */
    public function partitionsPaths() : array
    {
        return \array_map(
            fn ($implPath) => $this->createFromImplementation($implPath),
            $this->implementation->partitionsPaths()
        );
    }

    /**
     * Difference between Path::uri and Path::path is that Path::uri returns path with scheme and Path::path returns path without scheme.
     */
    public function path() : string
    {
        return $this->implementation->path();
    }

    public function protocol() : Protocol
    {
        return $this->implementation->protocol();
    }

    public function randomize() : self
    {
        return $this->createFromImplementation($this->implementation->randomize());
    }

    public function rootDirectoryName() : ?string
    {
        return $this->implementation->rootDirectoryName();
    }

    public function setExtension(string $extension) : self
    {
        return $this->createFromImplementation($this->implementation->setExtension($extension));
    }

    public function skipDirectories(int $count) : ?self
    {
        return ($newImplementation = $this->implementation->skipDirectories($count)) === null
            ? null
            : $this->createFromImplementation($newImplementation);
    }

    public function staticPart() : self
    {
        return $this->createFromImplementation($this->implementation->staticPart());
    }

    public function suffix(string $string) : self
    {
        return $this->createFromImplementation($this->implementation->suffix($string));
    }

    /**
     * Difference between Path::uri and Path::path is that Path::uri returns path with scheme and Path::path returns path without scheme.
     */
    public function uri() : string
    {
        return $this->implementation->uri();
    }

    private function createFromImplementation(WindowsPath|UnixPath $implementation) : self
    {
        $instance = new self('', []);
        $instance->implementation = $implementation;

        return $instance;
    }
}
