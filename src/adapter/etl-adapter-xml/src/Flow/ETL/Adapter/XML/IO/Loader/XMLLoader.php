<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\IO\Loader;

use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\IO\Loader\File\FileWrite;
use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\IO\Loader\File\FileWriterFactory;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\File\PartitionRouter;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function trim;

final class XMLLoader implements Closure, Discardable, FileLoader, FileWriterFactory, Loader
{
    private ?FileWrite $frame = null;

    private ?Part $task = null;

    private PartitionRouter $router;

    private Partitioning $partitioning;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private string $attributePrefix = '_';

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = 'Y-m-d\TH:i:s.uP';

    private ?XMLEncoder $encoder = null;

    private string $listElementName = 'element';

    private string $mapElementKeyName = 'key';

    private string $mapElementName = 'element';

    private string $mapElementValueName = 'value';

    private readonly Path $path;

    private string $rootElementName = 'root';

    private string $rowElementName = 'row';

    /**
     * @var array<string, string>
     */
    private array $xmlAttributes = ['version' => '1.0', 'encoding' => 'UTF-8'];

    public function __construct(
        Path $path,
        private readonly XMLWriter $xmlWriter,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $filesystem->supports($path) || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'to_xml');

        $this->filesystem = $filesystem;
        $this->partitioning = Partitioning::none();
        $this->router = new PartitionRouter($this->partitioning);
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::XML);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $loader = (new self(
            Path::fromArray(type_array()->assert($data['path'] ?? null)),
            $rebuild->of($data['xml_writer'] ?? null, XMLWriter::class),
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
        ))
            ->saveMode(type_enum(SaveMode::class)->assert(constant(
                SaveMode::class . '::' . type_string()->assert($data['save_mode'] ?? null),
            )))
            ->partitionBy(Partitioning::fromArray(type_array()->assert($data['partitioning'] ?? null)))
            ->withAttributePrefix(type_string()->assert($data['attribute_prefix'] ?? null))
            ->withDateFormat(type_string()->assert($data['date_format'] ?? null))
            ->withDateTimeFormat(type_string()->assert($data['date_time_format'] ?? null))
            ->withListElementName(type_string()->assert($data['list_element_name'] ?? null))
            ->withMapElementKeyName(type_string()->assert($data['map_element_key_name'] ?? null))
            ->withMapElementName(type_string()->assert($data['map_element_name'] ?? null))
            ->withMapElementValueName(type_string()->assert($data['map_element_value_name'] ?? null))
            ->withRootElementName(type_string()->assert($data['root_element_name'] ?? null))
            ->withRowElementName(type_string()->assert($data['row_element_name'] ?? null))
            ->withXMLAttributes(type_map(type_string(), type_string())->assert($data['xml_attributes'] ?? null));

        if (($data['task'] ?? null) !== null) {
            $loader->task = Part::fromArray(type_array()->assert($data['task']));
        }

        return $loader;
    }

    public function describe(): ?Description
    {
        if (!$this->xmlWriter instanceof Describable) {
            return null;
        }

        return Description::compose(self::class, [
            'path' => $this->path->normalize(),
            'filesystem' => new ConfiguredFilesystem($this->filesystem),
            'save_mode' => $this->saveMode->name,
            'partitioning' => $this->partitioning->normalize(),
            'task' => $this->task?->normalize(),
            'xml_writer' => $this->xmlWriter,
            'attribute_prefix' => $this->attributePrefix,
            'date_format' => $this->dateFormat,
            'date_time_format' => $this->dateTimeFormat,
            'list_element_name' => $this->listElementName,
            'map_element_key_name' => $this->mapElementKeyName,
            'map_element_name' => $this->mapElementName,
            'map_element_value_name' => $this->mapElementValueName,
            'root_element_name' => $this->rootElementName,
            'row_element_name' => $this->rowElementName,
            'xml_attributes' => $this->xmlAttributes,
        ]);
    }

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->partitioning = $partitioning;
        $this->router = new PartitionRouter($partitioning);

        return $this;
    }

    public function closure(): void
    {
        $this->frame()->closure();
        $this->frame = null;
    }

    public function discard(): void
    {
        $this->frame?->discard();
        $this->frame = null;
    }

    public function commit(string $run, int $tasks): ?FileCommit
    {
        return FileCommit::admitted(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->partitioning->by->count() > 0,
            $run,
            $tasks,
        );
    }

    public function destination(): Path
    {
        return $this->path;
    }

    public function partitioning(): Partitioning
    {
        return $this->partitioning;
    }

    public function inTask(Part $task): static
    {
        $loader = (new self($this->path, $this->xmlWriter, $this->filesystem))
            ->saveMode($this->saveMode)
            ->partitionBy($this->partitioning)
            ->withAttributePrefix($this->attributePrefix)
            ->withDateFormat($this->dateFormat)
            ->withDateTimeFormat($this->dateTimeFormat)
            ->withListElementName($this->listElementName)
            ->withMapElementKeyName($this->mapElementKeyName)
            ->withMapElementName($this->mapElementName)
            ->withMapElementValueName($this->mapElementValueName)
            ->withRootElementName($this->rootElementName)
            ->withRowElementName($this->rowElementName)
            ->withXMLAttributes($this->xmlAttributes);

        $loader->task = $task;

        return $loader;
    }

    public function load(Rows $rows, Backend $backend): void
    {
        $this->frame()->write($rows, $backend);
    }

    public function writer(DestinationStream $stream, Schema $schema, Backend $backend): FileWriter
    {
        $attributes = '';

        foreach ($this->xmlAttributes as $name => $value) {
            $attributes .= $name . '="' . $value . '" ';
        }

        return new XMLFileWriter(
            $stream,
            $this->encoder(),
            '<?xml ' . trim($attributes) . "?>\n<" . $this->rootElementName . ">\n",
            $this->rootElementName,
        );
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
    }

    public function withAttributePrefix(string $attributePrefix): self
    {
        $this->attributePrefix = $attributePrefix;

        return $this;
    }

    public function withDateFormat(string $dateFormat): self
    {
        $this->dateFormat = $dateFormat;

        return $this;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withListElementName(string $listElementName): self
    {
        $this->listElementName = $listElementName;

        return $this;
    }

    public function withMapElementKeyName(string $mapElementKeyName): self
    {
        $this->mapElementKeyName = $mapElementKeyName;

        return $this;
    }

    public function withMapElementName(string $mapElementName): self
    {
        $this->mapElementName = $mapElementName;

        return $this;
    }

    public function withMapElementValueName(string $mapElementValueName): self
    {
        $this->mapElementValueName = $mapElementValueName;

        return $this;
    }

    public function withRootElementName(string $rootElementName): self
    {
        $this->rootElementName = $rootElementName;

        return $this;
    }

    public function withRowElementName(string $rowElementName): self
    {
        $this->rowElementName = $rowElementName;

        return $this;
    }

    /**
     * @param array<string, string> $xmlAttributes
     */
    public function withXMLAttributes(array $xmlAttributes): self
    {
        $this->xmlAttributes = $xmlAttributes;

        return $this;
    }

    private function encoder(): XMLEncoder
    {
        return $this->encoder ??= new XMLEncoder(
            $this->xmlWriter,
            $this->attributePrefix,
            $this->dateTimeFormat,
            $this->dateFormat,
            $this->listElementName,
            $this->mapElementName,
            $this->mapElementKeyName,
            $this->mapElementValueName,
            $this->rowElementName,
        );
    }

    private function frame(): FileWrite
    {
        return $this->frame ??= new FileWrite(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->router,
            $this,
            $this->task,
        );
    }
}
