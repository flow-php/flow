<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Adapter\XML;

use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\XML;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class XMLReaderExtractorTest extends TestCase
{
    public function test_reading_xml_root() : void
    {
        $this->assertEquals(
            new Rows(
                Row::create(
                    Entry::array('row', [
                        'root' => [
                            'items' => [
                                'item' => [
                                    [
                                        'id' => [
                                            '@value' => 1,
                                            '@attributes' => [
                                                'id_attribute_01' => '1',
                                            ],
                                        ],
                                        '@attributes' => ['item_attribute_01' => '1'],
                                    ],
                                    [
                                        'id' => [
                                            '@value' => 2,
                                            '@attributes' => [
                                                'id_attribute_01' => '2',
                                            ],
                                        ],
                                        '@attributes' => ['item_attribute_01' => '2'],
                                    ],
                                    [
                                        'id' => [
                                            '@value' => 3,
                                            '@attributes' => [
                                                'id_attribute_01' => '3',
                                            ],
                                        ],
                                        '@attributes' => ['item_attribute_01' => '3'],
                                    ],
                                    [
                                        'id' => [
                                            '@value' => 4,
                                            '@attributes' => [
                                                'id_attribute_01' => '4',
                                            ],
                                        ],
                                        '@attributes' => ['item_attribute_01' => '4'],
                                    ],
                                    [
                                        'id' => [
                                            '@value' => 5,
                                            '@attributes' => [
                                                'id_attribute_01' => '5',
                                            ],
                                        ],
                                        '@attributes' => ['item_attribute_01' => '5'],
                                    ],
                                ],
                                '@attributes' => [
                                    'items_attribute_01' => '1',
                                    'items_attribute_02' => '2',
                                ],
                            ],
                            '@attributes' => [
                                'root_attribute_01' => '1',
                            ],
                        ],
                    ])
                )
            ),
            (new Flow())
                ->read(XML::from(__DIR__ . '/xml/simple_items.xml'))
                ->fetch()
        );
    }

    public function test_reading_xml_collection() : void
    {
        $this->assertEquals(
            new Rows(
                Row::create(
                    Entry::array('row', [
                        'items' => [
                            'item' => [
                                [
                                    'id' => [
                                        '@value' => 1,
                                        '@attributes' => [
                                            'id_attribute_01' => '1',
                                        ],
                                    ],
                                    '@attributes' => ['item_attribute_01' => '1'],
                                ],
                                [
                                    'id' => [
                                        '@value' => 2,
                                        '@attributes' => [
                                            'id_attribute_01' => '2',
                                        ],
                                    ],
                                    '@attributes' => ['item_attribute_01' => '2'],
                                ],
                                [
                                    'id' => [
                                        '@value' => 3,
                                        '@attributes' => [
                                            'id_attribute_01' => '3',
                                        ],
                                    ],
                                    '@attributes' => ['item_attribute_01' => '3'],
                                ],
                                [
                                    'id' => [
                                        '@value' => 4,
                                        '@attributes' => [
                                            'id_attribute_01' => '4',
                                        ],
                                    ],
                                    '@attributes' => ['item_attribute_01' => '4'],
                                ],
                                [
                                    'id' => [
                                        '@value' => 5,
                                        '@attributes' => [
                                            'id_attribute_01' => '5',
                                        ],
                                    ],
                                    '@attributes' => ['item_attribute_01' => '5'],
                                ],
                            ],
                            '@attributes' => [
                                'items_attribute_01' => '1',
                                'items_attribute_02' => '2',
                            ],
                        ],
                    ])
                )
            ),
            (new Flow())
                ->read(XML::from(__DIR__ . '/xml/simple_items.xml', 'root/items'))
                ->fetch()
        );
    }

    public function test_reading_xml_each_collection_item() : void
    {
        $this->assertEquals(
            new Rows(
                Row::create(
                    Entry::array('row', [
                        'item' => [
                            'id' => [
                                '@value' => 1,
                                '@attributes' => [
                                    'id_attribute_01' => '1',
                                ],
                            ],
                            '@attributes' => ['item_attribute_01' => '1'],
                        ],
                    ])
                ),
                Row::create(
                    Entry::array('row', [
                        'item' => [
                            'id' => [
                                '@value' => 2,
                                '@attributes' => [
                                    'id_attribute_01' => '2',
                                ],
                            ],
                            '@attributes' => ['item_attribute_01' => '2'],
                        ],
                    ])
                ),
                Row::create(
                    Entry::array('row', [
                        'item' => [
                            'id' => [
                                '@value' => 3,
                                '@attributes' => [
                                    'id_attribute_01' => '3',
                                ],
                            ],
                            '@attributes' => ['item_attribute_01' => '3'],
                        ],
                    ])
                ),
                Row::create(
                    Entry::array('row', [
                        'item' => [
                            'id' => [
                                '@value' => 4,
                                '@attributes' => [
                                    'id_attribute_01' => '4',
                                ],
                            ],
                            '@attributes' => ['item_attribute_01' => '4'],
                        ],
                    ])
                ),
                Row::create(
                    Entry::array('row', [
                        'item' => [
                            'id' => [
                                '@value' => 5,
                                '@attributes' => [
                                    'id_attribute_01' => '5',
                                ],
                            ],
                            '@attributes' => ['item_attribute_01' => '5'],
                        ],
                    ])
                )
            ),
            (new Flow())
                ->read(XML::from(__DIR__ . '/xml/simple_items.xml', 'root/items/item'))
                ->fetch()
        );
    }

    public function test_reading_xml_each_collection_item_id() : void
    {
        $this->assertEquals(
            new Rows(
                Row::create(
                    Entry::array('row', [
                        'id' => [
                            '@value' => '1',
                            '@attributes' => ['id_attribute_01' => '1'],
                        ],
                    ])
                ),
                Row::create(
                    Entry::array('row', [
                        'id' => [
                            '@value' => '2',
                            '@attributes' => ['id_attribute_01' => '2'],
                        ],
                    ])
                ),
                Row::create(
                    Entry::array('row', [
                        'id' => [
                            '@value' => '3',
                            '@attributes' => ['id_attribute_01' => '3'],
                        ],
                    ])
                ),
                Row::create(
                    Entry::array('row', [
                        'id' => [
                            '@value' => '4',
                            '@attributes' => ['id_attribute_01' => '4'],
                        ],
                    ])
                ),
                Row::create(
                    Entry::array('row', [
                        'id' => [
                            '@value' => '5',
                            '@attributes' => ['id_attribute_01' => '5'],
                        ],
                    ])
                )
            ),
            (new Flow())
                ->read(XML::from(__DIR__ . '/xml/simple_items.xml', 'root/items/item/id'))
                ->fetch()
        );
    }
}
