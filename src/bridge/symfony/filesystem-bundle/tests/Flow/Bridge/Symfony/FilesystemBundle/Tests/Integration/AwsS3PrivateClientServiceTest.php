<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use AsyncAws\S3\S3Client;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use Flow\Filesystem\FilesystemTable;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

final class AwsS3PrivateClientServiceTest extends KernelTestCase
{
    public function test_aws_s3_filesystem_resolves_private_client_service_id(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $clientDefinition = new Definition(S3Client::class);
                    $clientDefinition->setArguments([[
                        'accessKeyId' => 'key',
                        'accessKeySecret' => 'secret',
                        'region' => 'us-east-1',
                    ]]);
                    $clientDefinition->setPublic(false);

                    $container->setDefinition('async_aws.client.s3', $clientDefinition);
                });

                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'aws-s3' => [
                                    'type' => 'aws_s3',
                                    'bucket' => 'my-bucket',
                                    'client_service_id' => 'async_aws.client.s3',
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $table = $this->symfonyContext()->getService(FilesystemTable::class . ' $defaultFstab', FilesystemTable::class);

        static::assertInstanceOf(AsyncAWSS3Filesystem::class, $table->for('aws-s3'));
    }
}
