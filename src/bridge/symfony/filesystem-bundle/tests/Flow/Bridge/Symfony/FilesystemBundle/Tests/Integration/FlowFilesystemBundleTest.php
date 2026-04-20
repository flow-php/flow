<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemBundle\FlowFilesystemBundle;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;

final class FlowFilesystemBundleTest extends KernelTestCase
{
    public function test_kernel_boots_with_filesystem_bundle_registered() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'file' => ['type' => 'file'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        self::assertTrue($this->getContainer()->hasParameter('kernel.secret'));

        $bundles = $kernel->getBundles();
        self::assertArrayHasKey('FlowFilesystemBundle', $bundles);
        self::assertInstanceOf(FlowFilesystemBundle::class, $bundles['FlowFilesystemBundle']);
    }
}
