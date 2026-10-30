<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use DateTimeImmutable;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Generator;
use SensitiveParameter;

use function array_key_exists;
use function array_reverse;
use function bin2hex;
use function chmod;
use function dirname;
use function error_get_last;
use function file_exists;
use function filemtime;
use function filesize;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function in_array;
use function is_array;
use function is_dir;
use function is_file;
use function is_link;
use function is_string;
use function mkdir;
use function random_bytes;
use function rename;
use function rmdir;
use function scandir;
use function sprintf;
use function str_ends_with;
use function sys_get_temp_dir;
use function unlink;

/**
 * @type NativeLocalConfiguration = array{protocol: string, permissions: null|PermissionsShape}
 * @import-type PermissionsShape from Permissions
 *
 */
final readonly class NativeLocalFilesystem implements Filesystem
{
    public function __construct(
        private Mount $mount = new Mount('file'),
        private ?Permissions $permissions = null,
    ) {}

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        if (!array_key_exists('protocol', $configuration) || !is_string($configuration['protocol'])) {
            throw new InvalidArgumentException('NativeLocalFilesystem configuration must hold a "protocol" string');
        }

        if (
            !array_key_exists('permissions', $configuration)
            || $configuration['permissions'] !== null && !is_array($configuration['permissions'])
        ) {
            throw new InvalidArgumentException(
                'NativeLocalFilesystem configuration must hold "permissions", null or an array',
            );
        }

        return new self(
            new Mount($configuration['protocol']),
            $configuration['permissions'] === null ? null : Permissions::fromArray($configuration['permissions']),
        );
    }

    public function appendTo(Path $path): DestinationStream
    {
        $this->ensureWritable($path);

        return $this->withFilePermissions($path, NativeLocalDestinationStream::openAppend($path));
    }

    public function create(Path $path): DestinationStream
    {
        $this->ensureWritable($path);

        $temporary = $path->basenamePrefix('.' . bin2hex(random_bytes(8)) . '.');

        return $this->withFilePermissions($temporary, NativeLocalDestinationStream::openExclusive($path, $temporary));
    }

    /**
     * @return NativeLocalConfiguration
     */
    public function normalize(): array
    {
        return ['protocol' => $this->mount->protocol, 'permissions' => $this->permissions?->normalize()];
    }

    public function getSystemTmpDir(): Path
    {
        return path(sys_get_temp_dir());
    }

    public function list(Path $path, Filter $pathFilter = new OnlyFiles()): Generator
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$path->isPattern()) {
            if ($pathFilter->accept($status = self::statFor($path, $path->path()))) {
                yield $status;
            }

            return;
        }

        foreach ((new GlobWalker())->walk($path) as $filePath) {
            $status = self::statFor(path_real($filePath, $path->options()), $filePath);

            if ($pathFilter->accept($status)) {
                yield $status;
            }
        }
    }

    public function mount(): Mount
    {
        return $this->mount;
    }

    public function mv(Path $from, Path $to): bool
    {
        if (!$this->mount->supports($from)) {
            throw new InvalidSchemeException($from->protocol(), $this->mount->protocol);
        }
        if (!$this->mount->supports($to)) {
            throw new InvalidSchemeException($to->protocol(), $this->mount->protocol);
        }

        if (!file_exists($from->path())) {
            return false;
        }

        // rename() replaces a file with a file atomically; any directory on either side needs the target gone first
        if (is_dir($from->path()) || is_dir($to->path())) {
            $this->rm($to);
        }

        $this->createDirectory($to->parentDirectory()->path());

        return @rename($from->path(), $to->path());
    }

    public function readFrom(Path $path): SourceStream
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be open: " . $path->uri());
        }

        return NativeLocalSourceStream::open($path);
    }

    public function rm(Path $path, bool $recursive = true): bool
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$path->isPattern()) {
            if (!file_exists($path->path())) {
                return false;
            }

            if (is_dir($path->path())) {
                $this->rmdir($path->path(), $recursive);
            } else {
                unlink($path->path());
            }

            return true;
        }

        $deletedCount = 0;

        foreach (array_reverse((new GlobWalker())->walk($path)) as $filePath) {
            if (is_dir($filePath)) {
                $this->rmdir($filePath, $recursive);
            } else {
                unlink($filePath);
            }

            $deletedCount++;
        }

        return (bool) $deletedCount;
    }

    public function status(Path $path): ?FileStatus
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$path->isPattern()) {
            if (!file_exists($path->path())) {
                return null;
            }

            return self::statFor($path, $path->path());
        }

        $filePath = (new GlobWalker())->walk($path)[0] ?? null;

        return $filePath === null ? null : self::statFor(path($filePath, $path->options()), $filePath);
    }

    public function supports(Path $path): bool
    {
        return $this->mount->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        $this->ensureWritable($path);

        return $this->withFilePermissions($path, NativeLocalDestinationStream::openBlank($path));
    }

    private function createDirectory(string $directory): void
    {
        if (is_dir($directory)) {
            return;
        }

        $this->createDirectory(dirname($directory));

        if (!mkdir($directory) && !is_dir($directory)) {
            throw new RuntimeException(sprintf('Directory "%s" was not created', $directory));
        }

        if ($this->permissions !== null) {
            chmod($directory, $this->permissions->directory);
        }
    }

    private function ensureWritable(Path $path): void
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be written: " . $path->uri());
        }

        $this->createDirectory($path->parentDirectory()->path());
    }

    private function rmdir(string $dirPath, bool $recursive): void
    {
        if (is_link($dirPath)) {
            unlink($dirPath);

            return;
        }

        if (!is_dir($dirPath)) {
            throw new InvalidArgumentException("{$dirPath} must be a directory");
        }

        if (!$recursive) {
            if (!@rmdir($dirPath)) {
                throw new RuntimeException(
                    "Directory {$dirPath} was not removed: " . (error_get_last()['message'] ?? ''),
                );
            }

            return;
        }

        if (!str_ends_with($dirPath, '/')) {
            $dirPath .= '/';
        }

        $files = scandir($dirPath);

        if (!$files) {
            throw new RuntimeException("Can't read directory: {$dirPath}");
        }

        foreach ($files as $file) {
            if (in_array($file, ['.', '..'], true)) {
                continue;
            }

            $filePath = $dirPath . $file;

            if (is_link($filePath)) {
                unlink($filePath);

                continue;
            }

            if (is_dir($filePath)) {
                $this->rmdir($filePath, true);
            } else {
                unlink($filePath);
            }
        }

        rmdir($dirPath);
    }

    private function withFilePermissions(Path $file, NativeLocalDestinationStream $stream): NativeLocalDestinationStream
    {
        if ($this->permissions !== null) {
            chmod($file->path(), $this->permissions->file);
        }

        return $stream;
    }

    private static function statFor(Path $path, string $absolutePath): FileStatus
    {
        $isFile = is_file($absolutePath);
        $mtime = $isFile ? filemtime($absolutePath) : false;

        return new FileStatus(
            $path,
            $isFile,
            $isFile ? (filesize($absolutePath) ?: null) : null,
            $mtime !== false ? new DateTimeImmutable('@' . $mtime) : null,
        );
    }
}
