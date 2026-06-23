<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Context;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use RuntimeException;
use SplFileInfo;

use function array_key_exists;
use function explode;
use function fclose;
use function getenv;
use function implode;
use function is_dir;
use function is_executable;
use function is_file;
use function is_resource;
use function proc_close;
use function proc_open;
use function rmdir;
use function stream_get_contents;
use function sys_get_temp_dir;
use function trim;
use function uniqid;
use function unlink;

use const DIRECTORY_SEPARATOR;
use const PATH_SEPARATOR;

/**
 * Provides Git working copies for tests by building small local repositories,
 * so the suite never depends on network access to a remote fixture.
 */
final class GitContext
{
    public const BRANCH = '1.x';

    public const REPOSITORY_URL = 'https://github.com/flow-php/phpstan-types-bridge.git';

    public const TAG = '0.39.0';

    /**
     * @var array<string>
     */
    private array $directories = [];

    /**
     * @var array<string, string>
     */
    private array $revisions = [];

    public function cleanup(): void
    {
        foreach ($this->directories as $directory) {
            $this->removeDirectory($directory);
        }

        $this->directories = [];
        $this->revisions = [];
    }

    public function createLocalRepository(string $ref): string
    {
        $directory = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'flow_telemetry_git_' . uniqid();
        $this->directories[] = $directory;

        $this->git(['init', '--quiet', $directory]);
        $this->git(['-C', $directory, 'symbolic-ref', 'HEAD', 'refs/heads/' . self::BRANCH]);
        $this->git(['-C', $directory, 'config', 'user.email', 'ci@flow-php.com']);
        $this->git(['-C', $directory, 'config', 'user.name', 'Flow PHP']);
        $this->git(['-C', $directory, 'config', 'commit.gpgsign', 'false']);
        $this->git(['-C', $directory, 'remote', 'add', 'origin', self::REPOSITORY_URL]);
        $this->git(['-C', $directory, 'commit', '--quiet', '--allow-empty', '-m', 'fixture commit']);

        if ($ref === self::TAG) {
            $this->git(['-C', $directory, 'tag', self::TAG]);
            $this->git(['-C', $directory, 'checkout', '--quiet', self::TAG]);
        }

        $this->revisions[$directory] = $this->git(['-C', $directory, 'rev-parse', 'HEAD']);

        return $directory;
    }

    public function createLocalRepositoryWithRemote(string $remoteUrl): string
    {
        $directory = $this->createLocalRepository(self::BRANCH);

        $this->git(['-C', $directory, 'remote', 'set-url', 'origin', $remoteUrl]);

        return $directory;
    }

    public function headRevision(string $directory): string
    {
        if (!array_key_exists($directory, $this->revisions)) {
            throw new RuntimeException('No repository was created at ' . $directory);
        }

        return $this->revisions[$directory];
    }

    public function gitBinaryExists(): bool
    {
        return $this->run(['git', '--version']) !== null;
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

    /**
     * @param array<string> $arguments
     */
    private function git(array $arguments): string
    {
        $output = $this->run(['git', ...$arguments]);

        if ($output === null) {
            throw new RuntimeException('git ' . implode(' ', $arguments) . ' failed');
        }

        return trim($output);
    }

    /**
     * @param array<string> $command
     */
    private function run(array $command): ?string
    {
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $pipes = [];
        $process = @proc_open($command, $descriptors, $pipes);

        if (!is_resource($process)) {
            return null;
        }

        $stdin = $pipes[0] ?? null;
        $stdout = $pipes[1] ?? null;
        $stderr = $pipes[2] ?? null;

        if (is_resource($stdin)) {
            fclose($stdin);
        }

        $output = '';

        if (is_resource($stdout)) {
            $output = (string) stream_get_contents($stdout);
            fclose($stdout);
        }

        // Drain stderr so the child never blocks on a full pipe.
        if (is_resource($stderr)) {
            stream_get_contents($stderr);
            fclose($stderr);
        }

        return proc_close($process) === 0 ? $output : null;
    }
}
