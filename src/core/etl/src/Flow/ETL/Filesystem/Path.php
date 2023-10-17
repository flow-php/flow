<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Stream\ResourceContext;
use Flow\ETL\Partition;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{path: string, scheme: string, options: array<string, mixed>, extension: string|false}>
 */
final class Path implements Serializable
{
    private string|false $extension;

    private string $path;

    private string $scheme;

    /**
     * @param array<string, mixed> $options
     *
     * @throws InvalidArgumentException
     */
    public function __construct(string $uri, private readonly array $options = [])
    {
        $urlParts = \parse_url($uri);

        if (!\is_array($urlParts)) {
            throw new InvalidArgumentException("Invalid uri: {$uri}");
        }

        if (\array_key_exists('scheme', $urlParts) && !\in_array($urlParts['scheme'], \stream_get_wrappers(), true)) {
            throw new InvalidArgumentException("Unknown scheme \"{$urlParts['scheme']}\"");
        }

        $path = \array_key_exists('scheme', $urlParts)
            ? \str_replace($urlParts['scheme'] . '://', '', $uri)
            : $uri;

        if (\array_key_exists('scheme', $urlParts)) {
            $path = !\str_starts_with($path, DIRECTORY_SEPARATOR) ? (DIRECTORY_SEPARATOR . $path) : $path;
        } else {
            if (!\str_starts_with($path, DIRECTORY_SEPARATOR)) {
                throw new InvalidArgumentException("Relative paths are not supported, consider using instead Path::realpath: {$uri}");
            }

            if (\str_contains($path, '..' . DIRECTORY_SEPARATOR)) {
                throw new InvalidArgumentException("Relative paths are not supported, consider using instead Path::realpath: {$uri}");
            }
        }

        $this->path = $path;
        $this->scheme = \array_key_exists('scheme', $urlParts) ? $urlParts['scheme'] : 'file';
        $this->extension = \pathinfo($this->path)['extension'] ?? false;
    }

    /**
     * Turn relative path into absolute paths even when path does not exists or it's glob pattern.
     *
     * @param array<string, mixed> $options
     *
     * @throws InvalidArgumentException
     * @throws RuntimeException
     */
    public static function realpath(string $path, array $options = []) : self
    {
        // ""  - empty path is current, local directory
        if ('' === $path) {
            return new self(\getcwd() ?: '', $options);
        }

        // "non_local://path/to/file.txt" - non local paths can't be relative
        $urlParts = \parse_url($path);

        if (\is_array($urlParts) && \array_key_exists('scheme', $urlParts) && $urlParts['scheme'] !== 'file') {
            return new self($path, $options);
        }
        $realPath = $path;

        if ($realPath[0] === '~') {
            $homeEnv = \getenv('HOME');

            if (\is_string($homeEnv)) {
                $realPath = $homeEnv . DIRECTORY_SEPARATOR . \substr($realPath, 1);
            } else {
                // if HOME env is missing, fallback to posix functions

                if (!\function_exists('posix_getpwuid') || !\function_exists('posix_getuid')) {
                    throw new RuntimeException('Resolving homedir is not yet supported at OS :' . PHP_OS);
                }

                $userData = (array) \posix_getpwuid(\posix_getuid());

                if (!\is_string($userData['dir'] ?? null)) {
                    throw new RuntimeException("Can't resolve homedir for user executing script");
                }

                /** @psalm-suppress PossiblyUndefinedArrayOffset */
                $realPath = $userData['dir'] . DIRECTORY_SEPARATOR . \substr($realPath, 1);
            }
        }

        // "some/path/to/file.txt" - path not starting from / is relative to current dir
        $realPath = ($realPath[0] !== DIRECTORY_SEPARATOR)
            ? (\getcwd() . DIRECTORY_SEPARATOR . $realPath)
            : $realPath;

        /** @var array<string> $absoluteParts */
        $absoluteParts = [];

        foreach (\explode(DIRECTORY_SEPARATOR, $realPath) as $part) {
            if ($part === '.' || $part === '') {
                continue;
            }

            if ($part === '..') {
                if ([] !== $absoluteParts) {
                    \array_pop($absoluteParts);
                }

                continue;
            }

            $absoluteParts[] = $part;
        }

        /**
         * Make sure that realpath always start with /.
         */
        return new self(DIRECTORY_SEPARATOR . \implode(DIRECTORY_SEPARATOR, $absoluteParts), $options);
    }

    public function __serialize() : array
    {
        return [
            'scheme' => $this->scheme,
            'path' => $this->path,
            'options' => $this->options,
            'extension' => $this->extension,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->scheme = $data['scheme'];
        $this->options = $data['options'];
        $this->extension = $data['extension'];
    }

    public function addPartitions(Partition $partition, Partition ...$partitions) : self
    {
        \array_unshift($partitions, $partition);

        $partitionsPath = '';

        foreach ($partitions as $partition) {
            $partitionsPath .= DIRECTORY_SEPARATOR . $partition->name . '=' . $partition->value;
        }

        return new self($this->uri() . $partitionsPath, $this->options);
    }

    public function context() : ResourceContext
    {
        return ResourceContext::from($this);
    }

    public function extension() : string|false
    {
        return $this->extension;
    }

    public function isEqual(self $path) : bool
    {
        return $this->uri() === $path->uri()
            && $this->options === $path->options();
    }

    public function isLocal() : bool
    {
        return $this->scheme === 'file';
    }

    public function isPattern() : bool
    {
        return $this->isPathPattern($this->path);
    }

    public function matches(self $path) : bool
    {
        if (!$this->isPathPattern($this->path)) {
            return $this->isEqual($path);
        }

        if ($path->isPathPattern($path->path)) {
            return false;
        }

        return $this->fnmatch($this->path, $path->path);
    }

    /**
     * @return array<string, mixed>
     */
    public function options() : array
    {
        return $this->options;
    }

    public function parentDirectory() : self
    {
        if ($this->isPathPattern($this->path)) {
            throw new InvalidArgumentException("Can't take directory from path pattern.");
        }

        $path = \pathinfo($this->path);
        $dirname = \array_key_exists('dirname', $path) ? \ltrim($path['dirname'], DIRECTORY_SEPARATOR) : '';

        $dirname = $dirname === '' ? '/' : $dirname;

        return new self(
            $this->scheme . '://' . $dirname,
            $this->options
        );
    }

    /**
     * @return array<Partition>
     */
    public function partitions() : array
    {
        if ($this->isPathPattern($this->path)) {
            return [];
        }

        return Partition::fromUri($this->path);
    }

    public function path() : string
    {
        return $this->path;
    }

    public function randomize() : self
    {
        $extension = false !== $this->extension ? '.' . $this->extension : '';

        return new self(
            (\rtrim($this->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . \str_replace('.', '', \uniqid('', true)) . $extension),
            $this->options
        );
    }

    public function scheme() : string
    {
        return $this->scheme;
    }

    public function setExtension(string $extension) : self
    {
        if ($this->extension) {
            if ($this->extension === $extension) {
                return $this;
            }

            $pathinfo = \pathinfo($this->path);
            $path = ($pathinfo['dirname'] ?? '') . DIRECTORY_SEPARATOR . $pathinfo['filename'] . '.' . $extension;

            return new self($this->scheme . '://' . \ltrim($path, DIRECTORY_SEPARATOR), $this->options);
        }

        return new self($this->uri() . '.' . $extension, $this->options);
    }

    public function startsWith(self $path) : bool
    {
        return \str_starts_with($this->path, $path->path);
    }

    public function staticPart() : self
    {
        if (!$this->isPathPattern($this->path)) {
            return $this;
        }

        $pathInfo = \pathinfo($this->path);

        if (!\array_key_exists('dirname', $pathInfo) || $pathInfo['dirname'] === DIRECTORY_SEPARATOR) {
            return $this;
        }

        $staticPath = [];

        foreach (\array_filter(\explode(DIRECTORY_SEPARATOR, $pathInfo['dirname'])) as $folder) {
            if ($this->isPathPattern($folder)) {
                break;
            }

            $staticPath[] = $folder;
        }

        return new self($this->scheme() . '://' . \implode(DIRECTORY_SEPARATOR, $staticPath), $this->options);
    }

    public function uri() : string
    {
        return $this->scheme . '://' . \ltrim($this->path, DIRECTORY_SEPARATOR);
    }

    /**
     * Credits: https://github.com/Polycademy/upgradephp/blob/65c5a9be1e039bbc1ac83addaeba5bd875d758ea/upgrade.php#L2802.
     *
     * @psalm-suppress MixedAssignment
     * @psalm-suppress MixedArrayAccess
     * @psalm-suppress MixedArgument
     * @psalm-suppress MixedArrayAssignment
     */
    private function fnmatch(string $pattern, string $filename, int $flags = 0) : bool
    {
        if (\function_exists('fnmatch')) {
            return \fnmatch($pattern, $filename);
        }

        if ($flags & 4) {
            if (($filename[0] === '.') && ($pattern[0] !== '.')) {
                return false;
            }
        }

        $rxci = '';

        if ($flags & 16) {
            $rxci = 'i';
        }

        $wild = '.';

        if ($flags & 1) {
            $wild = '[^/' . DIRECTORY_SEPARATOR . DIRECTORY_SEPARATOR . ']';
        }

        static $cmp = [];

        if (isset($cmp["{$pattern}+{$flags}"])) {
            $rx = $cmp["{$pattern}+{$flags}"];
        } else {
            $rx = \preg_quote($pattern);
            $rx = \strtr($rx, [
                '\\*'=>"{$wild}*?", '\\?'=>"{$wild}", '\\['=>'[', '\\]'=>']',
            ]);
            $rx = '{^' . $rx . '$}' . $rxci;

            if (\count($cmp) >= 50) {
                $cmp = [];
            }
            $cmp["{$pattern}+{$flags}"] = $rx;
        }

        return (bool) (\preg_match($rx, $filename));
    }

    private function isPathPattern(string $path) : bool
    {
        if (\str_contains($path, '*') || \str_contains($path, '?')) {
            return true;
        }

        if (\str_contains($path, '[') && \str_contains($path, ']')) {
            return true;
        }

        if (\str_contains($path, '{') && \str_contains($path, '}')) {
            return true;
        }

        return false;
    }
}
