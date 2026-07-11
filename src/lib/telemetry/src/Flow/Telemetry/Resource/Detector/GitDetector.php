<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\VcsAttribute;
use Flow\Telemetry\Resource\ResourceDetector;

use function fclose;
use function feof;
use function fread;
use function getenv;
use function is_resource;
use function microtime;
use function proc_close;
use function proc_open;
use function proc_terminate;
use function stream_select;
use function stream_set_blocking;
use function strlen;
use function trim;

/**
 * Detects Git (VCS) information by shelling out to the `git` binary.
 *
 * Detects the following attributes (OpenTelemetry vcs.* semantic conventions):
 * - vcs.ref.head.revision: The current commit SHA
 * - vcs.ref.head.name: The current branch or tag name (omitted when HEAD is detached on an untagged commit)
 * - vcs.ref.head.type: The reference type ("branch" or "tag")
 * - vcs.repository.url.full: The remote "origin" URL (with any embedded credentials stripped)
 *
 * Requires the `git` binary to be available and the working directory (or the
 * provided one) to be inside a Git work tree. When neither is the case, an empty
 * Resource is returned.
 *
 * Example output:
 * ```
 * vcs.ref.head.revision: 9d59409acf479dfa0df1c3dad539b4825a8d2bcf
 * vcs.ref.head.name: main
 * vcs.ref.head.type: branch
 * vcs.repository.url.full: https://github.com/flow-php/flow.git
 * ```
 */
final readonly class GitDetector implements ResourceDetector
{
    private const MAX_OUTPUT_BYTES = 1_048_576;

    private const TIMEOUT_SECONDS = 10.0;

    private RemoteUrlSanitizer $remoteUrlSanitizer;

    public function __construct(
        private ?string $workingDirectory = null,
        private string $gitBinary = 'git',
    ) {
        $this->remoteUrlSanitizer = new RemoteUrlSanitizer();
    }

    public function detect(): Resource
    {
        if ($this->runGit(['rev-parse', '--is-inside-work-tree']) !== 'true') {
            return Resource::empty();
        }

        $attributes = [];

        $revision = $this->runGit(['rev-parse', 'HEAD']);

        if ($revision !== null) {
            $attributes[VcsAttribute::REF_HEAD_REVISION->value] = $revision;
        }

        $branch = $this->runGit(['rev-parse', '--abbrev-ref', 'HEAD']);

        if ($branch !== null && $branch !== 'HEAD') {
            $attributes[VcsAttribute::REF_HEAD_NAME->value] = $branch;
            $attributes[VcsAttribute::REF_HEAD_TYPE->value] = 'branch';
        } else {
            // Detached HEAD: resolve a tag pointing at the current commit, if any.
            $tag = $this->runGit(['describe', '--tags', '--exact-match']);

            if ($tag !== null) {
                $attributes[VcsAttribute::REF_HEAD_NAME->value] = $tag;
                $attributes[VcsAttribute::REF_HEAD_TYPE->value] = 'tag';
            }
        }

        $repositoryUrl = $this->runGit(['remote', 'get-url', 'origin']);

        if ($repositoryUrl !== null) {
            $attributes[VcsAttribute::REPOSITORY_URL->value] = $this->remoteUrlSanitizer->sanitize($repositoryUrl);
        }

        return Resource::create($attributes);
    }

    /**
     * @param array<string> $arguments
     */
    private function runGit(array $arguments): ?string
    {
        $command = [$this->gitBinary];

        if ($this->workingDirectory !== null) {
            $command[] = '-C';
            $command[] = $this->workingDirectory;
        }

        foreach ($arguments as $argument) {
            $command[] = $argument;
        }

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $environment = getenv();
        // Never let git block waiting for an interactive credential prompt.
        $environment['GIT_TERMINAL_PROMPT'] = '0';

        $pipes = [];
        $process = @proc_open($command, $descriptors, $pipes, null, $environment);

        if (!is_resource($process)) {
            return null;
        }

        $stdin = $pipes[0] ?? null;
        $stdout = $pipes[1] ?? null;
        $stderr = $pipes[2] ?? null;

        if (is_resource($stdin)) {
            fclose($stdin);
        }

        foreach ([$stdout, $stderr] as $pipe) {
            if (is_resource($pipe)) {
                stream_set_blocking($pipe, false);
            }
        }

        $output = '';
        $deadline = microtime(true) + self::TIMEOUT_SECONDS;
        $timedOut = false;

        while (is_resource($stdout) || is_resource($stderr)) {
            $remaining = $deadline - microtime(true);

            if ($remaining <= 0) {
                $timedOut = true;

                break;
            }

            $read = [];

            if (is_resource($stdout)) {
                $read[] = $stdout;
            }

            if (is_resource($stderr)) {
                $read[] = $stderr;
            }

            $write = [];
            $except = [];
            $seconds = (int) $remaining;

            if (
                @stream_select($read, $write, $except, $seconds, (int) (($remaining - $seconds) * 1_000_000)) === false
            ) {
                break;
            }

            foreach ($read as $pipe) {
                $chunk = fread($pipe, 8192);

                // Drain stderr without retaining it; cap stdout so a hostile config cannot exhaust memory.
                if (
                    $chunk !== false
                    && $chunk !== ''
                    && $pipe === $stdout
                    && strlen($output) < self::MAX_OUTPUT_BYTES
                ) {
                    $output .= $chunk;
                }

                if ($chunk === false || feof($pipe)) {
                    fclose($pipe);

                    if ($pipe === $stdout) {
                        $stdout = null;
                    } elseif ($pipe === $stderr) {
                        $stderr = null;
                    }
                }
            }
        }

        if ($timedOut) {
            proc_terminate($process, 9);
        }

        foreach ([$stdout, $stderr] as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }

        proc_close($process);

        if ($timedOut) {
            return null;
        }

        $output = trim($output);

        return $output === '' ? null : $output;
    }
}
