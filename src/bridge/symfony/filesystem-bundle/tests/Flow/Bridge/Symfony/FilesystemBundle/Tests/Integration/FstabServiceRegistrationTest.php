<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Filesystem\FilesystemTable;

final class FstabServiceRegistrationTest extends KernelTestCase
{
    public function test_fqcn_alias_resolves_to_default_fstab(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'default_fstab' => 'secondary',
                    'fstabs' => [
                        'primary' => [
                            'filesystems' => [
                                'file' => ['type' => 'file'],
                            ],
                        ],
                        'secondary' => [
                            'filesystems' => [
                                'memory' => ['type' => 'memory'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $table = $this->symfonyContext()->getService(FilesystemTable::class, FilesystemTable::class);

        static::assertSame('memory', $table->for('memory')->mount()->protocol);
    }

    public function test_named_argument_alias_resolves_to_fstab(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'memory' => ['type' => 'memory'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $aliasId = FilesystemTable::class . ' $defaultFstab';
        $table = $this->symfonyContext()->getService($aliasId, FilesystemTable::class);

        static::assertSame('memory', $table->for('memory')->mount()->protocol);
    }

    public function test_registers_default_fstab_with_multiple_filesystems(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'file' => ['type' => 'file'],
                                'memory' => ['type' => 'memory'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $table = $this->symfonyContext()->getService(FilesystemTable::class . ' $defaultFstab', FilesystemTable::class);

        static::assertSame('file', $table->for('file')->mount()->protocol);
        static::assertSame('memory', $table->for('memory')->mount()->protocol);
    }

    public function test_registers_secondary_fstab_separately(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'file' => ['type' => 'file'],
                                'memory' => ['type' => 'memory'],
                            ],
                        ],
                        'secondary' => [
                            'filesystems' => [
                                'memory' => ['type' => 'memory'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $default = $this->symfonyContext()->getService(
            FilesystemTable::class . ' $defaultFstab',
            FilesystemTable::class,
        );
        $secondary = $this->symfonyContext()->getService(
            FilesystemTable::class . ' $secondaryFstab',
            FilesystemTable::class,
        );

        static::assertNotSame($default, $secondary);
        static::assertCount(2, $default->filesystems());
        static::assertCount(1, $secondary->filesystems());
    }
}
