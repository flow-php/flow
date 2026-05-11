<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\StubBlobService;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Filesystem\Bridge\Azure\AzureBlobFilesystem;
use Flow\Filesystem\FilesystemTable;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

final class AzureBlobPrivateClientServiceTest extends KernelTestCase
{
    public function test_azure_blob_filesystem_resolves_private_client_service_id(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $clientDefinition = new Definition(StubBlobService::class);
                    $clientDefinition->setPublic(false);

                    $container->setDefinition('azure_storage.blob_service', $clientDefinition);
                });

                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'azure-blob' => [
                                    'type' => 'azure_blob',
                                    'container' => 'my-container',
                                    'client_service_id' => 'azure_storage.blob_service',
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $table = $this->symfonyContext()->getService(FilesystemTable::class . ' $defaultFstab', FilesystemTable::class);

        static::assertInstanceOf(AzureBlobFilesystem::class, $table->for('azure-blob'));
    }
}
