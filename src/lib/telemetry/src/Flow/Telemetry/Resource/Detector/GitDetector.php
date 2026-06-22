<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\VcsAttribute;
use Flow\Telemetry\Resource\ResourceDetector;

use function fclose;
use function is_resource;
use function is_string;
use function proc_close;
use function proc_open;
use function stream_get_contents;
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

        $output = is_resource($stdout) ? stream_get_contents($stdout) : null;

        if (is_resource($stdout)) {
            fclose($stdout);
        }

        // Drain and discard stderr so the child never blocks on a full pipe.
        if (is_resource($stderr)) {
            stream_get_contents($stderr);
            fclose($stderr);
        }

        proc_close($process);

        if (!is_string($output)) {
            return null;
        }

        $output = trim($output);

        return $output === '' ? null : $output;
    }
}
