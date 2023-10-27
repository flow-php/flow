<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Execution\Plan;

use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Pipeline\Execution\Plan\FilesystemOperations;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class FilesystemPlanTest extends TestCase
{
    public function test_get_destination_path_from_file_loader() : void
    {
        $plan = new FilesystemOperations(
            new Extractor\ProcessExtractor(),
            [
                new class implements FileLoader, Loader {
                    public function load(Rows $rows, FlowContext $context) : void
                    {
                    }

                    public function destination() : Path
                    {
                        return Path::realpath(__DIR__);
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                },
                new class implements FileLoader, Loader {
                    public function load(Rows $rows, FlowContext $context) : void
                    {
                    }

                    public function destination() : Path
                    {
                        return Path::realpath(__DIR__ . '/nested');
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                },
            ]
        );

        $this->assertEquals(
            [
                Path::realpath(__DIR__),
                Path::realpath(__DIR__ . '/nested'),
            ],
            [
                $plan->fileLoaders()[0]->destination(),
                $plan->fileLoaders()[1]->destination(),
            ]
        );
    }

    public function test_get_destination_path_from_non_file_loader() : void
    {
        $plan = new FilesystemOperations(
            new Extractor\ProcessExtractor(),
            [
                new class implements Loader {
                    public function load(Rows $rows, FlowContext $context) : void
                    {
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                },

            ]
        );

        $this->assertEquals(
            [],
            $plan->fileLoaders()
        );
    }

    public function test_get_destination_path_from_overriding_lodaer_with_file_loader() : void
    {
        $plan = new FilesystemOperations(
            new Extractor\ProcessExtractor(),
            [
                new class implements Loader, Loader\OverridingLoader {
                    public function load(Rows $rows, FlowContext $context) : void
                    {
                    }

                    public function loaders() : array
                    {
                        return [

                            new class implements FileLoader, Loader {
                                public function load(Rows $rows, FlowContext $context) : void
                                {
                                }

                                public function destination() : Path
                                {
                                    return Path::realpath(__DIR__);
                                }

                                public function __serialize() : array
                                {
                                    return [];
                                }

                                public function __unserialize(array $data) : void
                                {
                                }
                            },
                            new class implements FileLoader, Loader {
                                public function load(Rows $rows, FlowContext $context) : void
                                {
                                }

                                public function destination() : Path
                                {
                                    return Path::realpath(__DIR__ . '/nested');
                                }

                                public function __serialize() : array
                                {
                                    return [];
                                }

                                public function __unserialize(array $data) : void
                                {
                                }
                            },
                        ];
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                },
                new class implements Loader {
                    public function load(Rows $rows, FlowContext $context) : void
                    {
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                },
                new class implements FileLoader, Loader {
                    public function load(Rows $rows, FlowContext $context) : void
                    {
                    }

                    public function destination() : Path
                    {
                        return Path::realpath(__DIR__ . '/nested/nested');
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                },
            ]
        );

        $this->assertEquals(
            [
                Path::realpath(__DIR__),
                Path::realpath(__DIR__ . '/nested'),
                Path::realpath(__DIR__ . '/nested/nested'),
            ],
            [
                $plan->fileLoaders()[0]->destination(),
                $plan->fileLoaders()[1]->destination(),
                $plan->fileLoaders()[2]->destination(),
            ]
        );
    }

    public function test_get_source_path_from_file_extractor() : void
    {
        $plan = new FilesystemOperations(
            new class implements Extractor,
                Extractor\FileExtractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows();
                }

                public function source() : Path
                {
                    return Path::realpath(__DIR__);
                }
            },
            []
        );

        $this->assertEquals(
            Path::realpath(__DIR__),
            $plan->fileExtractors()[0]->source()
        );
    }

    public function test_get_source_path_from_non_file_extractor() : void
    {
        $plan = new FilesystemOperations(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows();
                }
            },
            []
        );

        $this->assertEquals(
            [],
            $plan->fileExtractors()
        );
    }

    public function test_get_source_path_from_overriding_extractor_with_file_extractor() : void
    {
        $plan = new FilesystemOperations(
            new class implements Extractor,
                Extractor\OverridingExtractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows();
                }

                public function extractors() : array
                {
                    return [
                        new class implements Extractor {
                            public function extract(FlowContext $context) : \Generator
                            {
                                yield new Rows();
                            }
                        },
                        new class implements Extractor, Extractor\FileExtractor {
                            public function extract(FlowContext $context) : \Generator
                            {
                                yield new Rows();
                            }

                            public function source() : Path
                            {
                                return Path::realpath(__DIR__);
                            }
                        },
                    ];
                }
            },
            []
        );

        $this->assertEquals(
            Path::realpath(__DIR__),
            $plan->fileExtractors()[0]->source()
        );
    }
}
