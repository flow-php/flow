<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Resource\Detector;

use Flow\Telemetry\Resource\Attribute\VcsAttribute;
use Flow\Telemetry\Resource\Detector\GitDetector;
use Flow\Telemetry\Tests\Context\GitContext;
use Flow\Telemetry\Tests\Integration\GitTestCase;

use function sys_get_temp_dir;
use function uniqid;

use const DIRECTORY_SEPARATOR;

final class GitDetectorTest extends GitTestCase
{
    public function test_detect_keeps_scp_like_ssh_remote_untouched(): void
    {
        $directory = $this->gitContext->cloneRepositoryWithRemote('git@github.com:flow-php/phpstan-types-bridge.git');

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(
            'git@github.com:flow-php/phpstan-types-bridge.git',
            $resource->get(VcsAttribute::REPOSITORY_URL->value),
        );
    }

    public function test_detect_returns_branch_name_and_type_when_not_detached(): void
    {
        $directory = $this->gitContext->cloneRepository(GitContext::BRANCH);

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(GitContext::BRANCH, $resource->get(VcsAttribute::REF_HEAD_NAME->value));
        static::assertSame('branch', $resource->get(VcsAttribute::REF_HEAD_TYPE->value));
    }

    public function test_detect_returns_empty_resource_outside_a_work_tree(): void
    {
        $directory = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'flow_telemetry_git_non_existing_' . uniqid();

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(0, $resource->count());
    }

    public function test_detect_returns_head_revision(): void
    {
        $directory = $this->gitContext->cloneRepository(GitContext::BRANCH);

        $resource = (new GitDetector($directory))->detect();

        $revision = $resource->get(VcsAttribute::REF_HEAD_REVISION->value);

        static::assertIsString($revision);
        static::assertMatchesRegularExpression('/^[0-9a-f]{40}$/', $revision);
    }

    public function test_detect_returns_repository_url(): void
    {
        $directory = $this->gitContext->cloneRepository(GitContext::BRANCH);

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(GitContext::REPOSITORY_URL, $resource->get(VcsAttribute::REPOSITORY_URL->value));
    }

    public function test_detect_returns_tag_name_and_type_in_detached_head(): void
    {
        $directory = $this->gitContext->cloneRepository(GitContext::TAG);

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(GitContext::TAG, $resource->get(VcsAttribute::REF_HEAD_NAME->value));
        static::assertSame('tag', $resource->get(VcsAttribute::REF_HEAD_TYPE->value));
        static::assertSame(GitContext::TAG_REVISION, $resource->get(VcsAttribute::REF_HEAD_REVISION->value));
    }

    public function test_detect_strips_credentials_from_repository_url(): void
    {
        $directory = $this->gitContext->cloneRepositoryWithRemote(
            'https://user:secret@github.com/flow-php/phpstan-types-bridge.git',
        );

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(
            'https://github.com/flow-php/phpstan-types-bridge.git',
            $resource->get(VcsAttribute::REPOSITORY_URL->value),
        );
    }

    public function test_detect_uses_explicit_git_binary_path(): void
    {
        $gitBinary = $this->gitContext->resolveGitBinaryPath();

        if ($gitBinary === null) {
            static::markTestSkipped('Unable to resolve an absolute git binary path');
        }

        $directory = $this->gitContext->cloneRepository(GitContext::BRANCH);

        $resource = (new GitDetector($directory, $gitBinary))->detect();

        static::assertSame(GitContext::BRANCH, $resource->get(VcsAttribute::REF_HEAD_NAME->value));
    }
}
