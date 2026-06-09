<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Resource\Detector;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use RuntimeException;
use SplFileInfo;

use function explode;
use function fclose;
use function getenv;
use function is_dir;
use function is_executable;
use function is_file;
use function is_resource;
use function proc_close;
use function proc_open;
use function rmdir;
use function stream_get_contents;
use function uniqid;
use function unlink;

use const DIRECTORY_SEPARATOR;
use const PATH_SEPARATOR;

/**
 * Provides Git working copies for tests by shallow-cloning a small, public
 * fixture repository.
 */
final class GitRepositoryHelper
{
    public const REPOSITORY_URL = 'https://github.com/flow-php/phpstan-types-bridge.git';
    public const BRANCH = '1.x';
    public const TAG = '0.39.0';
    public const TAG_REVISION = '4d135d4eff0d7895ad2d07737484474936fb5200';

    private static bool $repositoryUnavailable = false;

    /**
     * @var array<string>
     */
    private array $directories = [];

    public function gitBinaryExists(): bool
    {
        return $this->runGit(['--version']);
    }

    public function resolveGitBinaryPath(): ?string
    {
        $path = getenv('PATH');

        if ($path === false) {
            return null;
        }

        foreach (explode(PATH_SEPARATOR, $path) as $directory) {
            if ($directory === '') {
                continue;
            }

            $candidate = $directory . DIRECTORY_SEPARATOR . 'git';

            if (is_file($candidate) && is_executable($candidate)) {
                return $candidate;
            }
        }

        return null;
    }

    /**
     * Shallow-clones the fixture repository at the given branch or tag. Cloning
     * a tag leaves HEAD detached.
     *
     * `--no-tags` is required: the fixture repository points several release tags
     * at the same commit, so auto-following tags would let `git describe
     * --exact-match` resolve a different tag than the one requested.
     *
     * @throws RuntimeException when the repository cannot be cloned (e.g. no network access)
     */
    public function cloneRepository(string $ref): string
    {
        if (self::$repositoryUnavailable) {
            throw new RuntimeException('Fixture repository ' . self::REPOSITORY_URL . ' is unavailable');
        }

        $directory = __DIR__ . '/var/flow_telemetry_git_' . uniqid();
        $this->directories[] = $directory;

        if (!$this->runGit([
            'clone',
            '--quiet',
            '--depth',
            '1',
            '--no-tags',
            '--branch',
            $ref,
            self::REPOSITORY_URL,
            $directory,
        ])) {
            self::$repositoryUnavailable = true;

            throw new RuntimeException('Unable to clone fixture repository ' . self::REPOSITORY_URL);
        }

        return $directory;
    }

    /**
     * @throws RuntimeException when the repository cannot be cloned (e.g. no network access)
     */
    public function cloneRepositoryWithRemote(string $remoteUrl): string
    {
        $directory = $this->cloneRepository(self::BRANCH);

        $this->runGit(['-C', $directory, 'remote', 'set-url', 'origin', $remoteUrl]);

        return $directory;
    }

    public function cleanup(): void
    {
        foreach ($this->directories as $directory) {
            $this->removeDirectory($directory);
        }

        $this->directories = [];
    }

    /**
     * @param array<string> $arguments
     */
    private function runGit(array $arguments): bool
    {
        $command = ['git'];

        foreach ($arguments as $argument) {
            $command[] = $argument;
        }

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $pipes = [];
        $process = @proc_open($command, $descriptors, $pipes);

        if (!is_resource($process)) {
            return false;
        }

        $stdin = $pipes[0] ?? null;
        $stdout = $pipes[1] ?? null;
        $stderr = $pipes[2] ?? null;

        if (is_resource($stdin)) {
            fclose($stdin);
        }

        // Drain stdout and stderr so the child never blocks on a full pipe.
        if (is_resource($stdout)) {
            stream_get_contents($stdout);
            fclose($stdout);
        }

        if (is_resource($stderr)) {
            stream_get_contents($stderr);
            fclose($stderr);
        }

        return proc_close($process) === 0;
    }

    private function removeDirectory(string $directory): void
    {
        if (!is_dir($directory)) {
            return;
        }

        $items = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($directory, FilesystemIterator::SKIP_DOTS),
            RecursiveIteratorIterator::CHILD_FIRST,
        );

        foreach ($items as $item) {
            if (!$item instanceof SplFileInfo) {
                continue;
            }

            if ($item->isDir()) {
                rmdir($item->getPathname());
            } else {
                unlink($item->getPathname());
            }
        }

        rmdir($directory);
    }
}
