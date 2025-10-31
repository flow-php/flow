<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path\{Option, Options, UnixPath, WindowsPath};
use Flow\Filesystem\Stream\ResourceContext;

final readonly class Path
{
    public function __construct(private WindowsPath|UnixPath $implementation)
    {
    }

    /**
     * @param array<array-key, null|bool|float|int|string|\UnitEnum>|Options $options
     */
    public static function from(string $uri, array|Options $options = []) : self
    {
        return new self(
            \PHP_OS_FAMILY === 'Windows'
            ? new WindowsPath($uri, $options)
            : new UnixPath($uri, $options)
        );
    }

    /**
     * Turn relative path into absolute paths even when path does not exists or it's glob pattern.
     *
     * @param array<string, null|bool|float|int|string|\UnitEnum>|Options $options
     *
     * @throws RuntimeException
     */
    public static function realpath(string $path, array|Options $options = []) : self
    {
        return new self(
            \PHP_OS_FAMILY === 'Windows'
                ? WindowsPath::realpath($path, $options)
                : UnixPath::realpath($path, $options)
        );
    }

    public function addPartitions(Partition $partition, Partition ...$partitions) : self
    {
        return new self($this->implementation->addPartitions($partition, ...$partitions));
    }

    public function basename() : string
    {
        return $this->implementation->basename();
    }

    public function basenamePrefix(string $prefix) : self
    {
        return new self($this->implementation->basenamePrefix($prefix));
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

    public function getOption(string|Option $option, string|int|bool|float|\UnitEnum|null $default = null) : string|int|bool|float|\UnitEnum|null
    {
        return $this->implementation->options()->get($option, $default);
    }

    public function hasOption(string|Option $option) : bool
    {
        return $this->implementation->options()->has($option);
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

    /**
     * @return array<string, null|bool|float|int|string|\UnitEnum>
     */
    public function options() : array
    {
        return $this->implementation->options()->toArray();
    }

    public function parentDirectory() : self
    {
        return new self($this->implementation->parentDirectory());
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
            fn ($implPath) => new self($implPath),
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
        return new self($this->implementation->randomize());
    }

    public function rootDirectoryName() : ?string
    {
        return $this->implementation->rootDirectoryName();
    }

    public function setExtension(string $extension) : self
    {
        return new self($this->implementation->setExtension($extension));
    }

    public function setOption(string|Option $option, string|int|bool|float|\UnitEnum|null $value) : self
    {
        return new self(
            $this->implementation->withOptions(
                $this->implementation->options()->set(
                    $option instanceof Option ? $option->value : $option,
                    $value
                )
            )
        );
    }

    public function setOptionWhenEmpty(string|Option $option, string|int|bool|float|\UnitEnum|null $value) : self
    {
        return new self(
            $this->implementation->withOptions(
                $this->implementation->options()->setWhenEmpty(
                    $option,
                    $value
                )
            )
        );
    }

    public function skipDirectories(int $count) : ?self
    {
        return ($newImplementation = $this->implementation->skipDirectories($count)) === null
            ? null
            : new self($newImplementation);
    }

    public function staticPart() : self
    {
        return new self($this->implementation->staticPart());
    }

    public function suffix(string $string) : self
    {
        return new self($this->implementation->suffix($string));
    }

    /**
     * Difference between Path::uri and Path::path is that Path::uri returns path with scheme and Path::path returns path without scheme.
     */
    public function uri() : string
    {
        return $this->implementation->uri();
    }
}
