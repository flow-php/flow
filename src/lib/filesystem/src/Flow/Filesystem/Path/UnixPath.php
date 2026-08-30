<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use InvalidArgumentException as BaseInvalidArgumentException;

use function array_key_exists;
use function array_map;
use function array_pop;
use function array_slice;
use function array_unique;
use function array_values;
use function count;
use function explode;
use function function_exists;
use function getcwd;
use function getenv;
use function implode;
use function is_array;
use function is_string;
use function ltrim;
use function md5;
use function parse_url;
use function pathinfo;
use function posix_getpwuid;
use function posix_getuid;
use function preg_match;
use function preg_match_all;
use function preg_quote;
use function preg_replace;
use function random_int;
use function rtrim;
use function str_contains;
use function str_ends_with;
use function str_replace;
use function str_starts_with;
use function strtolower;
use function strtr;
use function substr;

use const PHP_INT_MAX;

final readonly class UnixPath
{
    private const string PARTITION_PLACEHOLDER_PATTERN = '/\{([^\/\\\\{}*?\[\]]+)\}/';

    private const string PLACEHOLDER_SENTINEL = "\x01";

    private Options $options;

    private string $path;

    private string $protocol;

    /**
     * @param array<array-key, null|bool|float|int|string|\UnitEnum>|Options $options
     */
    public function __construct(string $uri, array|Options $options = [])
    {
        $this->options = is_array($options) ? new Options($options) : $options;

        $matches = [];

        if (preg_match('/^([a-zA-Z0-9+-]+):\/\//', $uri, $matches)) {
            $this->protocol = $matches[1];
            $path = str_replace($matches[1] . '://', '', $uri);
        } else {
            $this->protocol = 'file';
            $path = $uri;
        }

        $this->path = $this->normalizePath($this->resolveHomePath($path));
    }

    /**
     * @param array<array-key, null|bool|float|int|string|\UnitEnum>|Options $options
     *
     * @throws RuntimeException
     */
    public static function realpath(string $path, array|Options $options = []): self
    {
        if ($path === '') {
            throw new InvalidArgumentException(
                'Empty path passed to UnixPath::realpath() — refusing to silently fall back to getcwd(). '
                . 'Pass an explicit non-empty path.',
            );
        }

        if (($urlParts = parse_url($path)) && array_key_exists('scheme', $urlParts)) {
            if ($urlParts['scheme'] !== 'file') {
                return new self($path, $options);
            }
            $path = $urlParts['path'] ?? '';
        }

        $realPath = $path;

        if ($realPath[0] === '~') {
            if (is_string($homeEnv = getenv('HOME'))) {
                $realPath = $homeEnv . '/' . substr($realPath, 1);
            } else {
                if (!function_exists('posix_getpwuid') || !function_exists('posix_getuid')) {
                    throw new RuntimeException('Resolving homedir is not yet supported at OS :' . PHP_OS);
                }

                if (!is_string(($userData = (array) posix_getpwuid(posix_getuid()))['dir'] ?? null)) {
                    throw new RuntimeException("Can't resolve homedir for user executing script");
                }

                if (!array_key_exists('dir', $userData)) {
                    throw new RuntimeException("Can't resolve homedir for user executing script");
                }

                $realPath = $userData['dir'] . '/' . substr($realPath, 1);
            }
        }

        if (!self::isUnixAbsolute($realPath)) {
            $cwd = getcwd();

            if ($cwd === false) {
                throw new RuntimeException('Cannot resolve current working directory');
            }

            $realPath = $cwd . '/' . $realPath;
        }

        $absoluteParts = [];

        foreach (explode('/', $realPath) as $part) {
            if ($part === '.' || $part === '') {
                continue;
            }

            if ($part === '..') {
                if ($absoluteParts !== []) {
                    array_pop($absoluteParts);
                }

                continue;
            }

            $absoluteParts[] = $part;
        }

        return new self('/' . implode('/', $absoluteParts), $options);
    }

    public function addPartitions(Partition $partition, Partition ...$partitions): self
    {
        if ($this->isPathPattern(preg_replace(self::PARTITION_PLACEHOLDER_PATTERN, '', $this->path) ?? $this->path)) {
            throw new InvalidArgumentException("Can't add partitions to path pattern.");
        }

        $partitions = [$partition, ...$partitions];
        $partitionNames = array_map(static fn(Partition $p) => $p->name, $partitions);
        $path = $this->path;

        foreach ($this->partitionPlaceholders() as $placeholder) {
            $placeholderPartition = null;

            foreach ($partitions as $index => $nextPartition) {
                if ($nextPartition->name === $placeholder) {
                    $placeholderPartition = $nextPartition;
                    unset($partitions[$index]);

                    break;
                }
            }

            if ($placeholderPartition === null) {
                throw new InvalidArgumentException(
                    'Path partition placeholder {'
                    . $placeholder
                    . "} does not match any partition, available partitions: '"
                    . implode("', '", $partitionNames)
                    . "'",
                );
            }

            $path = str_replace('{' . $placeholder . '}', $placeholderPartition->value, $path);
        }

        if (count($partitions) === 0) {
            return new self($this->protocol . '://' . $path, $this->options);
        }

        $pathInfo = pathinfo($path);
        $dirname = $pathInfo['dirname'] ?? '';
        $basename = $pathInfo['basename'];
        $partitionsString = implode('/', array_map(
            static fn(Partition $p) => $p->name . '=' . $p->value,
            array_values($partitions),
        ));

        return match ($dirname) {
            '', '.' => new self($this->protocol . ':///' . $partitionsString . '/' . $basename, $this->options),
            '/', '\\' => new self($this->protocol . ':///' . $partitionsString . '/' . $basename, $this->options),
            default => new self(
                $this->protocol . '://' . $dirname . '/' . $partitionsString . '/' . $basename,
                $this->options,
            ),
        };
    }

    public function basename(): string
    {
        return pathinfo($this->path, PATHINFO_BASENAME);
    }

    public function basenamePrefix(string $prefix): self
    {
        $pathInfo = pathinfo($this->path);
        $dirname = $pathInfo['dirname'] ?? '';
        $basename = $pathInfo['basename'];

        return new self(
            $this->protocol
            . '://'
            . ($dirname === '' || $dirname === '.' ? $prefix . $basename : $dirname . '/' . $prefix . $basename),
            $this->options,
        );
    }

    public function endsWith(string $string): bool
    {
        return str_ends_with($this->path, $string);
    }

    public function extension(): string|false
    {
        return ($extension = pathinfo($this->path, PATHINFO_EXTENSION)) === '' ? false : strtolower($extension);
    }

    /**
     * Extracts partitions from a concrete path by matching it against partition placeholders in this path.
     * Values that would not be valid partitions (e.g. produced by files not written through partitioning) are skipped.
     */
    public function extractPlaceholderPartitions(self $path): Partitions
    {
        $matches = [];
        preg_match_all(self::PARTITION_PLACEHOLDER_PATTERN, $this->path, $matches);
        $names = array_values($matches[1]);

        if ([] === $names || $path->isPattern()) {
            return new Partitions();
        }

        $rx = preg_quote(
            preg_replace(self::PARTITION_PLACEHOLDER_PATTERN, self::PLACEHOLDER_SENTINEL, $this->path) ?? $this->path,
            null,
        );
        $rx = str_replace('\\*\\*', '(?:.*)?', $rx);
        $rx = str_replace('\\*', '[^/]*', $rx);
        $rx = strtr($rx, ['\\?' => '[^/]', '\\[' => '[', '\\]' => ']']);
        $rx = str_replace(self::PLACEHOLDER_SENTINEL, '([^/]+)', $rx);

        $valueMatches = [];

        if (!preg_match('{^' . $rx . '$}', $path->path, $valueMatches)) {
            return new Partitions();
        }

        $values = [];

        foreach ($names as $index => $name) {
            $value = $valueMatches[$index + 1];

            if (array_key_exists($name, $values) && $values[$name] !== $value) {
                return new Partitions();
            }

            $values[$name] = $value;
        }

        $partitionsList = [];

        foreach ($values as $name => $value) {
            try {
                $partitionsList[] = new Partition($name, $value);
            } catch (InvalidArgumentException) {
            }
        }

        return new Partitions(...$partitionsList);
    }

    public function filename(): string
    {
        return pathinfo($this->path, PATHINFO_FILENAME);
    }

    /**
     * Path with partition placeholders replaced by glob wildcards, suitable for glob-based listing.
     */
    public function glob(): string
    {
        return preg_replace(self::PARTITION_PLACEHOLDER_PATTERN, '*', $this->path) ?? $this->path;
    }

    public function isEqual(self $path): bool
    {
        return $this->path === $path->path;
    }

    public function isPattern(): bool
    {
        return $this->isPathPattern($this->path);
    }

    public function matches(self $path): bool
    {
        if (!$this->isPattern()) {
            return $this->isEqual($path);
        }

        if ($path->isPattern()) {
            return false;
        }

        return $this->fnmatch($this->path, $path->path);
    }

    public function options(): Options
    {
        return $this->options;
    }

    public function parentDirectory(): self
    {
        if ($this->isPathPattern($this->path)) {
            throw new InvalidArgumentException("Can't take directory from path pattern.");
        }

        $dirname = pathinfo($this->path)['dirname'] ?? '';

        return match ($dirname) {
            '', '.', '/', '\\' => new self($this->protocol . ':///', $this->options),
            default => new self($this->protocol . '://' . $dirname, $this->options),
        };
    }

    /**
     * @return array<string>
     */
    public function partitionPlaceholders(): array
    {
        $matches = [];
        preg_match_all(self::PARTITION_PLACEHOLDER_PATTERN, $this->path, $matches);

        return array_values(array_unique($matches[1]));
    }

    public function partitions(): Partitions
    {
        if ($this->isPattern()) {
            return new Partitions();
        }

        $partitionsList = [];
        $matches = [];

        foreach (explode('/', $this->path) as $part) {
            if (preg_match('/^([^=]+)=([^=]+)$/', $part, $matches)) {
                $partitionsList[] = new Partition($matches[1], $matches[2]);
            }
        }

        return new Partitions(...$partitionsList);
    }

    /**
     * @return array<int, self>
     */
    public function partitionsPaths(): array
    {
        if (!($partitions = $this->partitions())->count()) {
            return [];
        }

        $paths = [];
        $currentPartitionsList = [];
        $dirname = pathinfo($this->path)['dirname'] ?? '';

        foreach ($partitions as $partition) {
            $currentPartitionsList[] = $partition;
            $partitionsString = implode('/', array_map(
                static fn(Partition $p) => $p->name . '=' . $p->value,
                $currentPartitionsList,
            ));

            if ($dirname === '' || $dirname === '.') {
                $pathPart = $partitionsString;
            } else {
                $replaced = preg_replace(
                    '#/' . preg_quote($partitionsString, '#') . '/.*$#',
                    '/' . $partitionsString,
                    $dirname,
                );

                if ($replaced === null) {
                    throw new RuntimeException('Failed to compute partitioned path');
                }

                $pathPart = $replaced;
            }

            $paths[] = new self($this->protocol . '://' . $pathPart, $this->options);
        }

        return $paths;
    }

    public function path(): string
    {
        return $this->path;
    }

    public function protocol(): string
    {
        return $this->protocol;
    }

    public function randomize(): self
    {
        $pathInfo = pathinfo($this->path);
        $dirname = $pathInfo['dirname'] ?? '';
        $filename = $pathInfo['filename'];
        $extension = $pathInfo['extension'] ?? '';

        $newFilename = $filename . '_' . substr(md5((string) random_int(0, PHP_INT_MAX)), 0, 10);
        $newBasename = $extension !== '' ? $newFilename . '.' . $extension : $newFilename;

        return new self(
            $this->protocol
            . '://'
            . ($dirname === '' || $dirname === '.' ? $newBasename : $dirname . '/' . $newBasename),
            $this->options,
        );
    }

    public function rootDirectoryName(): ?string
    {
        return ($pathParts = explode('/', ltrim($this->path, '/')))[0] !== '' && count($pathParts) > 1
            ? $pathParts[0]
            : null;
    }

    public function setExtension(string $extension): self
    {
        $pathInfo = pathinfo($this->path);
        $dirname = $pathInfo['dirname'] ?? '';
        $filename = $pathInfo['filename'];

        return new self(
            $this->protocol
            . '://'
            . ($dirname === '' || $dirname === '.' ? $filename : $dirname . '/' . $filename)
            . '.'
            . $extension,
            $this->options,
        );
    }

    public function skipDirectories(int $count): ?self
    {
        if ($count < 0) {
            throw new BaseInvalidArgumentException('The number of folders to skip must be non-negative.');
        }

        if (!($remainingParts = array_slice(explode('/', ltrim($this->path, '/')), $count))) {
            return null;
        }

        return new self($this->protocol . '://' . implode('/', $remainingParts), $this->options);
    }

    public function staticPart(): self
    {
        if (!$this->isPattern()) {
            return $this;
        }

        $staticParts = [];

        foreach (explode('/', ltrim($this->path, '/')) as $part) {
            if ($this->isPathPattern($part)) {
                break;
            }
            $staticParts[] = $part;
        }

        return new self(
            $this->protocol . '://' . (count($staticParts) === 0 ? '/' : ltrim('/' . implode('/', $staticParts), '/')),
            $this->options,
        );
    }

    public function suffix(string $string): self
    {
        return new self(
            $this->protocol
            . '://'
            . ($this->path === '/' ? '/' . ltrim($string, '/') : rtrim($this->path, '/') . '/' . ltrim($string, '/')),
            $this->options,
        );
    }

    public function uri(): string
    {
        return $this->protocol . '://' . ltrim($this->path, '/');
    }

    public function withOptions(Options $options): self
    {
        return new self($this->uri(), $options);
    }

    private function fnmatch(string $pattern, string $filename, int $flags = 0): bool
    {
        if ($flags & 4) {
            if ($filename[0] === '.' && $pattern[0] !== '.') {
                return false;
            }
        }

        $rx = preg_quote(
            preg_replace(self::PARTITION_PLACEHOLDER_PATTERN, self::PLACEHOLDER_SENTINEL, $pattern) ?? $pattern,
            null,
        );
        $rx = str_replace('\\*\\*', '(.*)?', $rx);
        $rx = str_replace('\\*', '[^/]*', $rx);
        $rx = strtr($rx, ['\\?' => '[^/]', '\\[' => '[', '\\]' => ']']);
        $rx = str_replace(self::PLACEHOLDER_SENTINEL, '[^/]+', $rx);
        $rx = '{^' . $rx . '$}' . ($flags & 16 ? 'i' : '');

        return (bool) preg_match($rx, $filename);
    }

    private function isAbsolutePath(string $path): bool
    {
        return str_starts_with($path, '/');
    }

    private function isPathPattern(string $path): bool
    {
        return (
            str_contains($path, '*')
            || str_contains($path, '?')
            || str_contains($path, '[')
            || str_contains($path, '{')
        );
    }

    private function normalizePath(string $path): string
    {
        if ($path === '') {
            return '/';
        }

        return $this->isAbsolutePath($path) ? $path : '/' . $path;
    }

    private function resolveHomePath(string $path): string
    {
        if ($path === '' || $path[0] !== '~') {
            return $path;
        }

        if (is_string($homeEnv = getenv('HOME'))) {
            return $homeEnv . '/' . substr($path, 1);
        }

        if (!function_exists('posix_getpwuid') || !function_exists('posix_getuid')) {
            throw new RuntimeException('Resolving homedir is not yet supported at OS :' . PHP_OS);
        }

        if (!is_string(($userData = (array) posix_getpwuid(posix_getuid()))['dir'] ?? null)) {
            throw new RuntimeException("Can't resolve homedir for user executing script");
        }

        if (!array_key_exists('dir', $userData)) {
            throw new RuntimeException("Can't resolve homedir for user executing script");
        }

        return $userData['dir'] . '/' . substr($path, 1);
    }

    private static function isUnixAbsolute(string $path): bool
    {
        return str_starts_with($path, '/');
    }
}
