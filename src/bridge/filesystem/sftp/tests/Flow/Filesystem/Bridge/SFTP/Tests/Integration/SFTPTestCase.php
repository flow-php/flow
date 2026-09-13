<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Integration;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Bridge\SFTP\Tests\Context\SFTPContext;
use Override;

abstract class SFTPTestCase extends FlowIntegrationTestCase
{
    private ?SFTPContext $sftpContext = null;

    public function sftpContext(): SFTPContext
    {
        return $this->sftpContext ??= new SFTPContext();
    }

    #[Override]
    protected function setUp(): void
    {
        parent::setUp();

        $this->sftpContext()->wipe();
    }

    #[Override]
    protected function tearDown(): void
    {
        $this->sftpContext()->wipe();

        parent::tearDown();
    }
}
