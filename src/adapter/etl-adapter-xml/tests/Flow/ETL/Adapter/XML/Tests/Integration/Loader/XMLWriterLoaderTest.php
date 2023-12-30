<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration\Loader;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\exception_if_exists;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ignore;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\xml_entry;
use Flow\ETL\Adapter\XML\Loader\XMLWriterLoader;
use Flow\ETL\Config;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class XMLWriterLoaderTest extends TestCase
{
    public function test_save_mode_throw_exception_on_partitioned_rows() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_xml_loader_exception_mode', true);

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(from_array([
                ['id' => 1, 'partition' => 'a'],
                ['id' => 2, 'partition' => 'a'],
                ['id' => 3, 'partition' => 'a'],
                ['id' => 4, 'partition' => 'b'],
                ['id' => 5, 'partition' => 'b'],
            ]))
            ->partitionBy(ref('partition'))
            ->mode(exception_if_exists())
            ->write(new XMLWriterLoader(Path::realpath($path)))
            ->run();

        $this->expectExceptionMessage('Destination path "file:/' . $path . '/partition=b" already exists, please change path to different or set different SaveMode');

        df()
            ->read(from_array([
                ['id' => 8, 'partition' => 'b'],
                ['id' => 10, 'partition' => 'b'],
            ]))
            ->partitionBy(ref('partition'))
            ->mode(SaveMode::ExceptionIfExists)
            ->write(new XMLWriterLoader(Path::realpath($path)))
            ->run();
    }

    public function test_save_unsupported_entry_types() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_xml_loader_exception_mode', true);

        if (\file_exists($path)) {
            \unlink($path);
        }

        $this->expectExceptionMessage('Entry of type Flow\ETL\Row\Entry\ListEntry cannot be normalized to XML values.');

        df()
            ->read(from_array([
                ['id' => 1, 'list' => ['a', 'b']],
                ['id' => 2, 'list' => ['c', 'd']],
            ]))
            ->mode(exception_if_exists())
            ->write(new XMLWriterLoader(Path::realpath($path)))
            ->run();
    }

    public function test_save_with_ignore_mode() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_xml_loader_ignore_mode', true) . '.xml';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]))
            ->saveMode(ignore())
            ->write(new XMLWriterLoader(Path::realpath($path)))
            ->run();

        df()
            ->read(from_array([
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ]))
            ->mode(SaveMode::Ignore)
            ->write(new XMLWriterLoader(Path::realpath($path)))
            ->run();

        $this->assertEquals(
            new Rows(Row::create(xml_entry('node', \file_get_contents($path)))),
            df()
                ->read(from_xml($path, 'rows'))
                ->sortBy(ref('id')->asc())
                ->fetch()
        );
    }

    public function test_xml_loader() : void
    {
        $stream = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_xml_loader', true) . '.xml';

        (new Flow())
            ->process(
                new Rows(
                    ...\array_map(
                        fn (int $i) : Row => Row::create(
                            new Row\Entry\IntegerEntry('id', $i),
                            new Row\Entry\StringEntry('name', 'name_' . $i),
                            new Row\Entry\StringEntry('special', 'one, two & three')
                        ),
                        \range(0, 10)
                    )
                )
            )
            ->sortBy(ref('id')->asc())
            ->write(new XMLWriterLoader(Path::realpath($stream)))
            ->run();

        $this->assertXmlStringEqualsXmlString(
            <<<'XML'
<?xml version="1.0"?>
<rows>
  <row>
    <id>0</id>
    <name>name_0</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>1</id>
    <name>name_1</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>2</id>
    <name>name_2</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>3</id>
    <name>name_3</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>4</id>
    <name>name_4</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>5</id>
    <name>name_5</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>6</id>
    <name>name_6</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>7</id>
    <name>name_7</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>8</id>
    <name>name_8</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>9</id>
    <name>name_9</name>
    <special>one, two &amp; three</special>
  </row>
  <row>
    <id>10</id>
    <name>name_10</name>
    <special>one, two &amp; three</special>
  </row>
</rows>
XML,
            \file_get_contents($stream)
        );

        if (\file_exists($stream)) {
            \unlink($stream);
        }
    }

    public function test_xml_loader_loading_empty_string() : void
    {
        $stream = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_xml_loader', true) . '.xml';

        $loader = new XMLWriterLoader(Path::realpath($stream));
        $loader->load(new Rows(), $context = new FlowContext(Config::default()));
        $loader->closure($context);

        $this->assertXmlStringEqualsXmlString(
            <<<'XML'
<?xml version="1.0"?>
<rows/>
XML,
            \file_get_contents($stream)
        );

        if (\file_exists($stream)) {
            \unlink($stream);
        }
    }

    public function test_xml_loader_with_a_thread_safe_and_overwrite() : void
    {
        $stream = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_xml_loader', true) . '.xml';

        $loader = new XMLWriterLoader(Path::realpath($stream));
        $loader->load(
            new Rows(
                ...\array_map(
                    fn (int $i) : Row => Row::create(
                        new Row\Entry\IntegerEntry('id', $i),
                        new Row\Entry\StringEntry('name', 'name_' . $i)
                    ),
                    \range(0, 5)
                )
            ),
            ($context = new FlowContext(Config::default()))->setAppendSafe()
        );

        $loader->load(
            new Rows(
                ...\array_map(
                    fn (int $i) : Row => Row::create(
                        new Row\Entry\IntegerEntry('id', $i),
                        new Row\Entry\StringEntry('name', 'name_' . $i)
                    ),
                    \range(6, 10)
                )
            ),
            $context = $context->setAppendSafe()
        );

        $loader->closure($context);

        $files = \array_values(\array_diff(\scandir($stream), ['..', '.']));

        $this->assertXmlStringEqualsXmlString(
            <<<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<rows>
    <row>
        <id>0</id>
        <name>name_0</name>
    </row>
    <row>
        <id>1</id>
        <name>name_1</name>
    </row>
    <row>
        <id>2</id>
        <name>name_2</name>
    </row>
    <row>
        <id>3</id>
        <name>name_3</name>
    </row>
    <row>
        <id>4</id>
        <name>name_4</name>
    </row>
    <row>
        <id>5</id>
        <name>name_5</name>
    </row>
    <row>
        <id>6</id>
        <name>name_6</name>
    </row>
    <row>
        <id>7</id>
        <name>name_7</name>
    </row>
    <row>
        <id>8</id>
        <name>name_8</name>
    </row>
    <row>
        <id>9</id>
        <name>name_9</name>
    </row>
    <row>
        <id>10</id>
        <name>name_10</name>
    </row>
</rows>
XML,
            \file_get_contents($stream . DIRECTORY_SEPARATOR . $files[0])
        );

        if (\file_exists($stream . DIRECTORY_SEPARATOR . $files[0])) {
            \unlink($stream . DIRECTORY_SEPARATOR . $files[0]);
        }
    }

    public function test_xml_loader_with_partitioning() : void
    {
        $path = \sys_get_temp_dir() . '/' . \str_replace('.', '', \uniqid('partitioned_', true));

        (new Flow())
            ->process(
                new Rows(
                    Row::create(int_entry('id', 1), int_entry('group', 1)),
                    Row::create(int_entry('id', 2), int_entry('group', 1)),
                    Row::create(int_entry('id', 3), int_entry('group', 2)),
                    Row::create(int_entry('id', 4), int_entry('group', 2)),
                )
            )
            ->partitionBy('group')
            ->load(new XMLWriterLoader(Path::realpath($path), collectionName: 'items', collectionElementName: 'item'))
            ->run();

        $partitions = \array_values(\array_diff(\scandir($path), ['..', '.']));

        $this->assertSame(
            [
                'group=1',
                'group=2',
            ],
            $partitions
        );

        $group1 = \array_values(\array_diff(\scandir($path . DIRECTORY_SEPARATOR . 'group=1'), ['..', '.']))[0];
        $group2 = \array_values(\array_diff(\scandir($path . DIRECTORY_SEPARATOR . 'group=2'), ['..', '.']))[0];

        $this->assertXmlStringEqualsXmlString(
            <<<'XML'
<?xml version="1.0" encoding="utf-8"?>
<items><item><id>1</id><group>1</group></item><item><id>2</id><group>1</group></item></items>
XML,
            \file_get_contents($path . DIRECTORY_SEPARATOR . 'group=1' . DIRECTORY_SEPARATOR . $group1)
        );
        $this->assertXmlStringEqualsXmlString(
            <<<'XML'
<?xml version="1.0" encoding="utf-8"?>
<items><item><id>3</id><group>2</group></item><item><id>4</id><group>2</group></item></items>
XML,
            \file_get_contents($path . DIRECTORY_SEPARATOR . 'group=2' . DIRECTORY_SEPARATOR . $group2)
        );
    }
}
