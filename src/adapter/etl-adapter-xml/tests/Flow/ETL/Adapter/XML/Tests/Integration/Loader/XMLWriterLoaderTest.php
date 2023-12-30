<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration\Loader;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{df, exception_if_exists, from_array, ignore, ref, xml_entry};
use Flow\ETL\Adapter\XML\Loader\XMLWriterLoader;
use Flow\ETL\{Config, Filesystem\SaveMode, Flow, FlowContext, Row, Rows};
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class XMLWriterLoaderTest extends TestCase
{
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
            ->saveMode(SaveMode::Ignore)
            ->write(new XMLWriterLoader(Path::realpath($path)))
            ->run();

        self::assertEquals(
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

        self::assertXmlStringEqualsXmlString(
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

        self::assertXmlStringEqualsXmlString(
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
}
