<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Filesystem\{FilesystemTable, Protocol};

final class FstabServiceRegistrationTest extends KernelTestCase
{
    public function test_fqcn_alias_resolves_to_default_fstab() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'default_fstab' => 'secondary',
                    'fstabs' => [
                        'primary' => [
                            'filesystems' => [
                                'file' => [],
                            ],
                        ],
                        'secondary' => [
                            'filesystems' => [
                                'memory' => [],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $table = $this->symfonyContext()->getService(FilesystemTable::class, FilesystemTable::class);

        self::assertSame('memory', $table->for(new Protocol('memory'))->protocol()->name);
    }

    public function test_named_argument_alias_resolves_to_fstab() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'memory' => [],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $aliasId = FilesystemTable::class . ' $defaultFstab';
        $table = $this->symfonyContext()->getService($aliasId, FilesystemTable::class);

        self::assertSame('memory', $table->for(new Protocol('memory'))->protocol()->name);
    }

    public function test_registers_default_fstab_with_multiple_filesystems() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'file' => [],
                                'memory' => [],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $table = $this->symfonyContext()->getService(FilesystemTable::class . ' $defaultFstab', FilesystemTable::class);

        self::assertSame('file', $table->for(new Protocol('file'))->protocol()->name);
        self::assertSame('memory', $table->for(new Protocol('memory'))->protocol()->name);
    }

    public function test_registers_secondary_fstab_separately() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => [
                                'file' => [],
                                'memory' => [],
                            ],
                        ],
                        'secondary' => [
                            'filesystems' => [
                                'memory' => [],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $default = $this->symfonyContext()->getService(FilesystemTable::class . ' $defaultFstab', FilesystemTable::class);
        $secondary = $this->symfonyContext()->getService(FilesystemTable::class . ' $secondaryFstab', FilesystemTable::class);

        self::assertNotSame($default, $secondary);
        self::assertCount(2, $default->filesystems());
        self::assertCount(1, $secondary->filesystems());
    }
}
