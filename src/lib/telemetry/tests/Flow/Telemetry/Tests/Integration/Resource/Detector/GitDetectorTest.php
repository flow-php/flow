<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Resource\Detector;

use Flow\Telemetry\Resource\Attribute\VcsAttribute;
use Flow\Telemetry\Resource\Detector\GitDetector;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function uniqid;

final class GitDetectorTest extends TestCase
{
    private GitRepositoryHelper $gitRepositoryHelper;

    protected function setUp(): void
    {
        $this->gitRepositoryHelper = new GitRepositoryHelper();

        if (!$this->gitRepositoryHelper->gitBinaryExists()) {
            static::markTestSkipped('Git binary is unavailable');
        }
    }

    protected function tearDown(): void
    {
        $this->gitRepositoryHelper->cleanup();
    }

    public function test_detect_returns_empty_resource_outside_a_work_tree(): void
    {
        $directory = __DIR__ . '/var/non_existing_' . uniqid();

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(0, $resource->count());
    }

    public function test_detect_returns_head_revision(): void
    {
        $directory = $this->cloneRepository(GitRepositoryHelper::BRANCH);

        $resource = (new GitDetector($directory))->detect();

        $revision = $resource->get(VcsAttribute::REF_HEAD_REVISION->value);

        static::assertIsString($revision);
        static::assertMatchesRegularExpression('/^[0-9a-f]{40}$/', $revision);
    }

    public function test_detect_returns_branch_name_and_type_when_not_detached(): void
    {
        $directory = $this->cloneRepository(GitRepositoryHelper::BRANCH);

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(GitRepositoryHelper::BRANCH, $resource->get(VcsAttribute::REF_HEAD_NAME->value));
        static::assertSame('branch', $resource->get(VcsAttribute::REF_HEAD_TYPE->value));
    }

    public function test_detect_returns_tag_name_and_type_in_detached_head(): void
    {
        $directory = $this->cloneRepository(GitRepositoryHelper::TAG);

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(GitRepositoryHelper::TAG, $resource->get(VcsAttribute::REF_HEAD_NAME->value));
        static::assertSame('tag', $resource->get(VcsAttribute::REF_HEAD_TYPE->value));
        static::assertSame(GitRepositoryHelper::TAG_REVISION, $resource->get(VcsAttribute::REF_HEAD_REVISION->value));
    }

    public function test_detect_returns_repository_url(): void
    {
        $directory = $this->cloneRepository(GitRepositoryHelper::BRANCH);

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(GitRepositoryHelper::REPOSITORY_URL, $resource->get(VcsAttribute::REPOSITORY_URL->value));
    }

    public function test_detect_strips_credentials_from_repository_url(): void
    {
        $directory = $this->cloneRepositoryWithRemote(
            'https://user:secret@github.com/flow-php/phpstan-types-bridge.git',
        );

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(
            'https://github.com/flow-php/phpstan-types-bridge.git',
            $resource->get(VcsAttribute::REPOSITORY_URL->value),
        );
    }

    public function test_detect_keeps_scp_like_ssh_remote_untouched(): void
    {
        $directory = $this->cloneRepositoryWithRemote('git@github.com:flow-php/phpstan-types-bridge.git');

        $resource = (new GitDetector($directory))->detect();

        static::assertSame(
            'git@github.com:flow-php/phpstan-types-bridge.git',
            $resource->get(VcsAttribute::REPOSITORY_URL->value),
        );
    }

    public function test_detect_uses_explicit_git_binary_path(): void
    {
        $gitBinary = $this->gitRepositoryHelper->resolveGitBinaryPath();

        if ($gitBinary === null) {
            static::markTestSkipped('Unable to resolve an absolute git binary path');
        }

        $directory = $this->cloneRepository(GitRepositoryHelper::BRANCH);

        $resource = (new GitDetector($directory, $gitBinary))->detect();

        static::assertSame(GitRepositoryHelper::BRANCH, $resource->get(VcsAttribute::REF_HEAD_NAME->value));
    }

    private function cloneRepository(string $ref): string
    {
        try {
            return $this->gitRepositoryHelper->cloneRepository($ref);
        } catch (RuntimeException) {
            static::markTestSkipped('Unable to clone the fixture repository ' . GitRepositoryHelper::REPOSITORY_URL);
        }
    }

    private function cloneRepositoryWithRemote(string $remoteUrl): string
    {
        try {
            return $this->gitRepositoryHelper->cloneRepositoryWithRemote($remoteUrl);
        } catch (RuntimeException) {
            static::markTestSkipped('Unable to clone the fixture repository ' . GitRepositoryHelper::REPOSITORY_URL);
        }
    }
}
