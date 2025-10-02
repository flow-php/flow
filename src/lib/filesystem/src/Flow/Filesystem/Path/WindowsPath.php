<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path;

use function Flow\Types\DSL\type_string;
use Flow\Filesystem\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Filesystem\{Partition, Partitions, Protocol};

final readonly class WindowsPath
{
    private Options $options;

    private string $path;

    private Protocol $protocol;

    /**
     * @param array<array-key, mixed>|Options $options
     */
    public function __construct(string $uri, array|Options $options = [])
    {
        $this->options = \is_array($options) ? new Options($options) : $options;

        if (\preg_match('/^([a-zA-Z0-9+-]+):\/\//', $uri, $matches)) {
            $this->protocol = new Protocol($matches[1]);
            $path = \str_replace($matches[1] . '://', '', $uri);
        } else {
            $this->protocol = new Protocol('file');
            $path = $uri;
        }

        $this->path = $this->normalizePath($this->resolveHomePath($path));
    }

    /**
     * @param array<array-key, mixed>|Options $options
     */
    public static function realpath(string $path, array|Options $options = []) : self
    {
        if ($path === '') {
            return new self(\str_replace('\\', '/', \getcwd() ?: ''), $options);
        }

        if (($urlParts = \parse_url($path)) && \array_key_exists('scheme', $urlParts) && $urlParts['scheme'] !== 'file') {
            return new self($path, $options);
        }

        $realPath = \str_replace('\\', '/', $path);

        if ($realPath !== '' && $realPath[0] === '~') {
            if (!($homeDir = \getenv('USERPROFILE') ?: (\getenv('HOMEDRIVE') && \getenv('HOMEPATH') ? \getenv('HOMEDRIVE') . \getenv('HOMEPATH') : null))) {
                throw new RuntimeException('Cannot resolve home directory on Windows');
            }

            $realPath = \str_replace('\\', '/', type_string()->assert($homeDir)) . '/' . \substr($realPath, 1);
        }

        if (!self::isWindowsAbsolute($realPath)) {
            $realPath = \str_replace('\\', '/', type_string()->assert(\getcwd())) . '/' . $realPath;
        }

        $drive = '';

        if (\preg_match('/^([a-zA-Z]):(.*)$/', $realPath, $matches)) {
            $drive = $matches[1] . ':';
            $realPath = $matches[2];
        }

        $absoluteParts = [];

        foreach (\explode('/', $realPath) as $part) {
            if ($part === '.' || $part === '') {
                continue;
            }

            if ($part === '..') {
                if ($absoluteParts !== []) {
                    \array_pop($absoluteParts);
                }

                continue;
            }

            $absoluteParts[] = $part;
        }

        return new self($drive . '/' . \implode('/', $absoluteParts), $options);
    }

    public function addPartitions(Partition $partition, Partition ...$partitions) : self
    {
        if ($this->isPattern()) {
            throw new InvalidArgumentException("Can't add partitions to path pattern.");
        }

        $pathInfo = \pathinfo($this->path);
        $dirname = $pathInfo['dirname'] ?? '';
        $basename = $pathInfo['basename'] ?? '';
        $partitionsString = \implode('/', \array_map(fn (Partition $p) => $p->name . '=' . $p->value, [$partition, ...$partitions]));

        return match ($dirname) {
            '', '.', '/', '\\' => new self($this->protocol->scheme() . '/' . $partitionsString . '/' . $basename, $this->options),
            default => new self(
                $this->protocol->scheme() . (\preg_match('/^[a-zA-Z]:[\\\\\/]?$/', $dirname)
                    ? \rtrim($dirname, '\\/') . '/' . $partitionsString . '/' . $basename
                    : $dirname . '/' . $partitionsString . '/' . $basename),
                $this->options
            ),
        };
    }

    public function basename() : string
    {
        return \pathinfo($this->path, PATHINFO_BASENAME);
    }

    public function basenamePrefix(string $prefix) : self
    {
        $pathInfo = \pathinfo($this->path);
        $dirname = $pathInfo['dirname'] ?? '';
        $basename = $pathInfo['basename'] ?? '';

        return new self(
            $this->protocol->scheme() . (($dirname === '' || $dirname === '.') ? $prefix . $basename : $dirname . '/' . $prefix . $basename),
            $this->options
        );
    }

    public function endsWith(string $string) : bool
    {
        return \str_ends_with($this->path, $string);
    }

    public function extension() : string|false
    {
        return ($extension = \pathinfo($this->path, PATHINFO_EXTENSION)) === '' ? false : \strtolower($extension);
    }

    public function filename() : string
    {
        return \pathinfo($this->path, PATHINFO_FILENAME);
    }

    public function isEqual(self $path) : bool
    {
        return $this->path === $path->path;
    }

    public function isPattern() : bool
    {
        return $this->isPathPattern($this->path);
    }

    public function matches(self $path) : bool
    {
        if (!$this->isPattern()) {
            return $this->isEqual($path);
        }

        if ($path->isPattern()) {
            return false;
        }

        return $this->fnmatch($this->path, $path->path);
    }

    public function options() : Options
    {
        return $this->options;
    }

    public function parentDirectory() : self
    {
        if ($this->isPathPattern($this->path)) {
            throw new InvalidArgumentException("Can't take directory from path pattern.");
        }

        $dirname = \pathinfo($this->path)['dirname'] ?? '';

        return match ($dirname) {
            '', '.', '/', '\\' => new self($this->protocol->scheme() . '/', $this->options),
            default => new self(
                $this->protocol->scheme() . (\preg_match('/^[a-zA-Z]:[\\\\\/]?$/', $dirname) ? \rtrim($dirname, '\\/') . '/' : $dirname),
                $this->options
            ),
        };
    }

    public function partitions() : Partitions
    {
        if ($this->isPattern()) {
            return new Partitions();
        }

        $partitionsList = [];

        foreach (\explode('/', $this->path) as $part) {
            if (\preg_match('/^([^=]+)=([^=]+)$/', $part, $matches)) {
                $partitionsList[] = new Partition($matches[1], $matches[2]);
            }
        }

        return new Partitions(...$partitionsList);
    }

    /**
     * @return array<int, self>
     */
    public function partitionsPaths() : array
    {
        if (!($partitions = $this->partitions())->count()) {
            return [];
        }

        $paths = [];
        $currentPartitionsList = [];
        $dirname = \pathinfo($this->path)['dirname'] ?? '';

        foreach ($partitions as $partition) {
            $currentPartitionsList[] = $partition;
            $partitionsString = \implode('/', \array_map(fn (Partition $p) => $p->name . '=' . $p->value, $currentPartitionsList));

            $paths[] = new self(
                $this->protocol->scheme() . (($dirname === '' || $dirname === '.')
                    ? $partitionsString
                    : \preg_replace('#/' . \preg_quote($partitionsString, '#') . '/.*$#', '/' . $partitionsString, $dirname)),
                $this->options
            );
        }

        return $paths;
    }

    public function path() : string
    {
        return $this->path;
    }

    public function protocol() : Protocol
    {
        return $this->protocol;
    }

    public function randomize() : self
    {
        $pathInfo = \pathinfo($this->path);
        $dirname = $pathInfo['dirname'] ?? '';
        $filename = $pathInfo['filename'] ?? '';
        $extension = $pathInfo['extension'] ?? '';

        $newFilename = $filename . '_' . \substr(\md5((string) \random_int(0, \PHP_INT_MAX)), 0, 10);
        $newBasename = $extension !== '' ? $newFilename . '.' . $extension : $newFilename;

        return new self(
            $this->protocol->scheme() . (($dirname === '' || $dirname === '.') ? $newBasename : $dirname . '/' . $newBasename),
            $this->options
        );
    }

    public function rootDirectoryName() : ?string
    {
        if (\preg_match('/^[a-zA-Z]:\/(.+)/', $this->path, $matches)) {
            return ($parts = \explode('/', $matches[1]))[0] !== '' ? $parts[0] : null;
        }

        if (\str_starts_with($this->path, '//')) {
            return ($parts = \explode('/', \ltrim($this->path, '/')))[0] !== '' ? $parts[0] : null;
        }

        return ($pathParts = \explode('/', \ltrim($this->path, '/')))[0] !== '' && \count($pathParts) > 1 ? $pathParts[0] : null;
    }

    public function setExtension(string $extension) : self
    {
        $pathInfo = \pathinfo($this->path);
        $dirname = $pathInfo['dirname'] ?? '';
        $filename = $pathInfo['filename'] ?? '';

        return new self(
            $this->protocol->scheme() . (($dirname === '' || $dirname === '.') ? $filename : $dirname . '/' . $filename) . '.' . $extension,
            $this->options
        );
    }

    public function skipDirectories(int $count) : ?self
    {
        if ($count < 0) {
            throw new \InvalidArgumentException('The number of folders to skip must be non-negative.');
        }

        if (\preg_match('/^([a-zA-Z]:)\/(.*)$/', $this->path, $matches)) {
            if ($matches[2] === '') {
                return null;
            }

            if (!($remainingParts = \array_slice(\explode('/', $matches[2]), $count))) {
                return null;
            }

            return new self($this->protocol->scheme() . $matches[1] . '/' . \implode('/', $remainingParts), $this->options);
        }

        if (!($remainingParts = \array_slice(\explode('/', \ltrim($this->path, '/')), $count))) {
            return null;
        }

        return new self($this->protocol->scheme() . \implode('/', $remainingParts), $this->options);
    }

    public function staticPart() : self
    {
        if (!$this->isPattern()) {
            return $this;
        }

        $staticParts = [];

        foreach (\explode('/', \ltrim($this->path, '/')) as $part) {
            if ($this->isPathPattern($part)) {
                break;
            }
            $staticParts[] = $part;
        }

        return new self(
            $this->protocol->scheme() . (\count($staticParts) === 0 ? '/' : \ltrim('/' . \implode('/', $staticParts), '/')),
            $this->options
        );
    }

    public function suffix(string $string) : self
    {
        return new self(
            $this->protocol->scheme() . ($this->path === '/' ? '/' . \ltrim($string, '/') : \rtrim($this->path, '/') . '/' . \ltrim($string, '/')),
            $this->options
        );
    }

    public function uri() : string
    {
        return $this->protocol->scheme() . \ltrim($this->path, '/');
    }

    private function fnmatch(string $pattern, string $filename, int $flags = 0) : bool
    {
        if ($flags & 4) {
            if (($filename[0] === '.') && ($pattern[0] !== '.')) {
                return false;
            }
        }

        static $cmp = [];

        if (isset($cmp["{$pattern}+{$flags}"])) {
            return (bool) \preg_match($cmp["{$pattern}+{$flags}"], $filename);
        }

        $rx = \preg_quote($pattern, null);
        $rx = \str_replace('\\*\\*', '(.*)?', $rx);
        $rx = \str_replace('\\*', '[^/]*', $rx);
        $rx = \strtr($rx, ['\\?' => '[^/]', '\\[' => '[', '\\]' => ']']);
        $rx = '{^' . $rx . '$}' . (($flags & 16) ? 'i' : '');

        if (\count($cmp) >= 50) {
            $cmp = [];
        }
        $cmp["{$pattern}+{$flags}"] = $rx;

        return (bool) \preg_match($rx, $filename);
    }

    private function isAbsolutePath(string $path) : bool
    {
        return \preg_match('/^[a-zA-Z]:[\\\\\/]/', $path) === 1
            || \str_starts_with($path, '\\\\')
            || \str_starts_with($path, '//')
            || \str_starts_with($path, '/');
    }

    private function isPathPattern(string $path) : bool
    {
        return \str_contains($path, '*')
            || \str_contains($path, '?')
            || \str_contains($path, '[')
            || \str_contains($path, '{');
    }

    private function normalizePath(string $path) : string
    {
        if ($path === '') {
            return '/';
        }

        $path = \str_replace('\\', '/', $path);

        return $this->isAbsolutePath($path) ? $path : '/' . $path;
    }

    private function resolveHomePath(string $path) : string
    {
        if ($path === '' || $path[0] !== '~') {
            return $path;
        }

        if (!($homeDir = \getenv('USERPROFILE') ?: (\getenv('HOMEDRIVE') && \getenv('HOMEPATH') ? \getenv('HOMEDRIVE') . \getenv('HOMEPATH') : null))) {
            throw new RuntimeException('Cannot resolve home directory on Windows');
        }

        return \str_replace('\\', '/', $homeDir) . '/' . \substr($path, 1);
    }

    private static function isWindowsAbsolute(string $path) : bool
    {
        return \preg_match('/^[a-zA-Z]:[\\\\\/]/', $path) === 1
            || \str_starts_with($path, '//')
            || \str_starts_with($path, '\\\\');
    }
}
