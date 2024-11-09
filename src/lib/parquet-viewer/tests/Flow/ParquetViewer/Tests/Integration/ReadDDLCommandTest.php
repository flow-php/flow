<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Integration;

use Flow\ParquetViewer\Parquet;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\ApplicationTester;

final class ReadDDLCommandTest extends TestCase
{
    public function test_read_data_command() : void
    {
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $path = \realpath(__DIR__ . '/../../Fixtures/flow.parquet');

        $tester = new ApplicationTester($application);
        $tester->run([
            'command' => 'read:ddl',
            'file' => $path,
        ]);

        self::assertSame(0, $tester->getStatusCode());
        self::assertStringContainsString(
            <<<'OUTPUT'
Parquet file DDL
================

{
    "schema": {
        "type": "message",
        "children": {
            "boolean": {
                "type": "BOOLEAN",
                "optional": true
            },
            "int32": {
                "type": "INT64",
                "optional": true
            },
            "int64": {
                "type": "INT64",
                "optional": true
            },
            "float": {
                "type": "FLOAT",
                "optional": true
            },
            "double": {
                "type": "FLOAT",
                "optional": true
            },
            "decimal": {
                "type": "FLOAT",
                "optional": true
            },
            "string": {
                "type": "BYTE_ARRAY (STRING)",
                "optional": true
            },
            "date": {
                "type": "INT64 (TIMESTAMP)",
                "optional": true
            },
            "datetime": {
                "type": "INT64 (TIMESTAMP)",
                "optional": true
            },
            "list_of_datetimes": {
                "type": "group",
                "optional": true,
                "children": {
                    "list": {
                        "type": "group",
                        "optional": false,
                        "children": {
                            "element": {
                                "type": "INT64 (TIMESTAMP)",
                                "optional": true
                            }
                        }
                    }
                }
            },
            "map_of_ints": {
                "type": "group",
                "optional": true,
                "children": {
                    "a": {
                        "type": "INT64",
                        "optional": true
                    },
                    "b": {
                        "type": "INT64",
                        "optional": true
                    },
                    "c": {
                        "type": "INT64",
                        "optional": true
                    }
                }
            },
            "list_of_strings": {
                "type": "group",
                "optional": true,
                "children": {
                    "list": {
                        "type": "group",
                        "optional": false,
                        "children": {
                            "element": {
                                "type": "BYTE_ARRAY (STRING)",
                                "optional": true
                            }
                        }
                    }
                }
            },
            "struct_flat": {
                "type": "group",
                "optional": true,
                "children": {
                    "id": {
                        "type": "INT64",
                        "optional": true
                    },
                    "name": {
                        "type": "BYTE_ARRAY (STRING)",
                        "optional": true
                    }
                }
            }
        }
    }
}
OUTPUT,
            $tester->getDisplay()
        );
    }
}
