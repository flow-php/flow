<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Filesystem\Path\Options;
use Flow\Filesystem\Stream\ResourceContext;

final class Path
{
    private string $basename;

    private string|false $extension;

    private string $filename;

    private ?bool $isPattern = null;

    private Options $options;

    private ?Partitions $partitions = null;

    private string $path;

    private Protocol $protocol;

    /**
     * @param array<string, mixed>|Options $options
     */
    public function __construct(string $uri, array|Options $options = [])
    {
        $scheme = \preg_match('/^([a-zA-Z0-9+-]+):\/\//', $uri, $matches) ? $matches[1] : 'file';

        $path = \str_replace($scheme . '://', '', $uri);

        if (!\str_starts_with($path, DIRECTORY_SEPARATOR)) {
            $path = DIRECTORY_SEPARATOR . $path;
        }

        $pathInfo = \pathinfo($path);

        $this->path = $path;
        $this->protocol = new Protocol($scheme);
        $this->extension = \array_key_exists('extension', $pathInfo) ? \strtolower($pathInfo['extension']) : false;
        $this->filename = $pathInfo['filename'];
        $this->basename = $pathInfo['basename'];
        $this->options = \is_array($options) ? new Options($options) : $options;
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

    public function addPartitions(Partition $partition, Partition ...$partitions) : self
    {
        if ($this->isPattern()) {
            throw new InvalidArgumentException("Can't add partitions to path pattern.");
        }

        \array_unshift($partitions, $partition);

        $partitionsPath = '';

        foreach ($partitions as $nextPartition) {
            $partitionsPath .= DIRECTORY_SEPARATOR . $nextPartition->name . '=' . $nextPartition->value;
        }

        /**
         * @psalm-suppress PossiblyFalseArgument
         *
         * @phpstan-ignore-next-line
         */
        $base = \trim(\mb_substr($this->path(), 0, \mb_strrpos($this->path(), $this->basename())), DIRECTORY_SEPARATOR);

        return new self($this->protocol->scheme() . $base . $partitionsPath . DIRECTORY_SEPARATOR . $this->basename(), $this->options);
    }

    public function basename() : string
    {
        return $this->basename;
    }

    public function basenamePrefix(string $prefix) : self
    {
        return new self(
            $this->parentDirectory()->uri() . DIRECTORY_SEPARATOR . $prefix . $this->basename(),
            $this->options()
        );
    }

    public function context() : ResourceContext
    {
        return ResourceContext::from($this);
    }

    /**
     * @psalm-assert-if-true string $this->extension
     */
    public function extension() : string|false
    {
        return $this->extension;
    }

    public function filename() : string
    {
        return $this->filename;
    }

    public function isEqual(self $path) : bool
    {
        return $this->uri() === $path->uri()
            && $this->options->toArray() === $path->options()->toArray();
    }

    public function isLocal() : bool
    {
        return $this->protocol->is('file');
    }

    public function isPattern() : bool
    {
        if ($this->isPattern !== null) {
            return $this->isPattern;
        }

        $this->isPattern = $this->isPathPattern($this->path);

        return $this->isPattern;
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

    public function options() : Options
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
            $this->protocol->scheme() . $dirname,
            $this->options
        );
    }

    public function partitions() : Partitions
    {
        if ($this->partitions === null) {
            if ($this->isPathPattern($this->path)) {
                $this->partitions = new Partitions();
            } else {
                $this->partitions = Partition::fromUri($this->path);
            }
        }

        return $this->partitions;
    }

    public function partitionsPaths() : array
    {
        $partitionPaths = [];

        foreach ($this->partitions() as $partition) {
            $partitionFolder = $partition->name . '=' . $partition->value;
            /** @psalm-suppress PossiblyFalseOperand */
            $partitionFolderPos = \mb_strpos($this->uri(), $partitionFolder) + \mb_strlen($partitionFolder);

            $partitionPaths[] = new self(\mb_substr($this->uri(), 0, $partitionFolderPos), $this->options);
        }

        return $partitionPaths;
    }

    /**
     * Difference between Path::uri and Path::path is that Path::uri returns path with scheme and Path::path returns path without scheme.
     */
    public function path() : string
    {
        return $this->path;
    }

    public function protocol() : Protocol
    {
        return $this->protocol;
    }

    /**
     * @psalm-suppress PossiblyFalseArgument
     */
    public function randomize() : self
    {
        $extension = false !== $this->extension ? '.' . $this->extension : '';

        /** @phpstan-ignore-next-line */
        $base = \trim(\mb_substr($this->path(), 0, \mb_strrpos($this->path(), $this->basename())), DIRECTORY_SEPARATOR);

        return new self(
            $this->protocol->scheme() . $base . DIRECTORY_SEPARATOR . $this->filename . '_' . \Flow\ETL\DSL\generate_random_string() . $extension,
            $this->options
        );
    }

    public function setExtension(string $extension) : self
    {
        return new self(
            $this->protocol->scheme() . $this->parentDirectory()->uri() . DIRECTORY_SEPARATOR . $this->filename . '.' . $extension,
            $this->options
        );
    }

    public function staticPart() : self
    {
        if (!$this->isPathPattern($this->path)) {
            return $this;
        }

        $pathInfo = \pathinfo($this->path);

        if (!\array_key_exists('dirname', $pathInfo) || $pathInfo['dirname'] === DIRECTORY_SEPARATOR) {
            if ($this->isPathPattern($pathInfo['basename'])) {
                return new self($this->protocol->scheme() . DIRECTORY_SEPARATOR, $this->options);
            }

            return $this;
        }

        $staticPath = [];

        foreach (\array_filter(\explode(DIRECTORY_SEPARATOR, $pathInfo['dirname'])) as $folder) {
            if ($this->isPathPattern($folder)) {
                break;
            }

            $staticPath[] = $folder;
        }

        return new self($this->protocol->scheme() . \implode(DIRECTORY_SEPARATOR, $staticPath), $this->options);
    }

    public function suffix(string $string) : self
    {
        return new self(\rtrim($this->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . \ltrim($string, DIRECTORY_SEPARATOR), $this->options);
    }

    /**
     * Difference between Path::uri and Path::path is that Path::uri returns path with scheme and Path::path returns path without scheme.
     */
    public function uri() : string
    {
        return $this->protocol->scheme() . \ltrim($this->path, DIRECTORY_SEPARATOR);
    }

    /**
     * Modified function from: https://github.com/Polycademy/upgradephp/blob/65c5a9be1e039bbc1ac83addaeba5bd875d758ea/upgrade.php#L2802.
     * This modified version is detecting double ** and single * in the same pattern.
     */
    private function fnmatch(string $pattern, string $filename, int $flags = 0) : bool
    {
        if ($flags & 4) {
            if (($filename[0] === '.') && ($pattern[0] !== '.')) {
                return false;
            }
        }

        $rxci = '';

        if ($flags & 16) {
            $rxci = 'i';
        }

        static $cmp = [];

        if (isset($cmp["{$pattern}+{$flags}"])) {
            $rx = $cmp["{$pattern}+{$flags}"];
        } else {
            $rx = \preg_quote($pattern, null);
            // Replace '**' with a regex that matches any number of directories
            $rx = \str_replace('\\*\\*', '(.*)?', $rx);
            // Replace '*' with a regex that matches any character except for directory separators
            $rx = \str_replace('\\*', '[^/]*', $rx);

            // Handle other special characters
            $rx = \strtr($rx, ['\\?' => '[^/]', '\\[' => '[', '\\]' => ']']);
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
