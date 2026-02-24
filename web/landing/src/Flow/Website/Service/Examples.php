<?php

declare(strict_types=1);

namespace Flow\Website\Service;

use function Flow\Types\DSL\type_string;
use Symfony\Component\Yaml\Yaml;

final readonly class Examples
{
    public function __construct(private string $examplesPath)
    {
    }

    public function code(string $topic, string $example, ?string $option = null) : string
    {
        if ($option !== null) {
            $path = \sprintf('%s/topics/%s/%s/%s/code.php', \realpath($this->examplesPath), $topic, $example, $option);
        } elseif ($this->hasOptions($topic, $example)) {
            $options = $this->options($topic, $example);

            if (0 === \count($options)) {
                throw new \RuntimeException(\sprintf('Example "%s" in topic "%s" has no valid options.', $example, $topic));
            }
            $firstOption = \current($options);
            $path = \sprintf('%s/topics/%s/%s/%s/code.php', \realpath($this->examplesPath), $topic, $example, $firstOption);
        } else {
            $path = \sprintf('%s/topics/%s/%s/code.php', \realpath($this->examplesPath), $topic, $example);
        }

        if (false === \file_exists($path)) {
            throw new \RuntimeException(\sprintf('Code example doesn\'t exists, it should be located in path: "%s".', $path));
        }

        return \file_get_contents($path);
    }

    /**
     * Returns the filesystem path for a data file in an example's input directory.
     */
    public function dataFilePath(string $topic, string $example, string $relativePath, ?string $option = null) : ?string
    {
        $basePath = $this->examplePath($topic, $example, $option);
        $filePath = $basePath . '/' . $relativePath;

        if (!\file_exists($filePath) || !\is_file($filePath)) {
            return null;
        }

        return $filePath;
    }

    /**
     * Returns a map of virtual paths to filesystem paths for data files in an example's input directory.
     *
     * @return array<string, string> Map of virtual path (e.g., 'input/dataset.csv') to relative filesystem path
     */
    public function dataFiles(string $topic, string $example, ?string $option = null) : array
    {
        $basePath = $this->examplePath($topic, $example, $option);
        $inputDir = $basePath . '/input';

        if (!\is_dir($inputDir)) {
            return [];
        }

        $files = [];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($inputDir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::LEAVES_ONLY
        );

        foreach ($iterator as $file) {
            if ($file->isFile()) {
                $virtualPath = \str_replace($basePath . '/', '', $file->getPathname());
                $files[$virtualPath] = $virtualPath;
            }
        }

        return $files;
    }

    public function description(string $topic, string $example, ?string $option = null) : ?string
    {
        if ($option !== null) {
            $path = \sprintf('%s/topics/%s/%s/%s/description.md', \realpath($this->examplesPath), $topic, $example, $option);
        } elseif ($this->hasOptions($topic, $example)) {
            $options = $this->options($topic, $example);

            if (0 === \count($options)) {
                return null;
            }
            $firstOption = \current($options);
            $path = \sprintf('%s/topics/%s/%s/%s/description.md', \realpath($this->examplesPath), $topic, $example, $firstOption);
        } else {
            $path = \sprintf('%s/topics/%s/%s/description.md', \realpath($this->examplesPath), $topic, $example);
        }

        if (false === \file_exists($path)) {
            return null;
        }

        return \file_get_contents($path);
    }

    /**
     * @return array<string>
     */
    public function examples(string $topic) : array
    {
        $path = \sprintf('%s/topics/%s', \realpath($this->examplesPath), $topic);

        if (false === \file_exists($path)) {
            throw new \RuntimeException(\sprintf('Topic "%s" doesn\'t exists, it should be located in path: "%s".', $topic, $path));
        }

        $examples = \array_values(\array_diff(\scandir($path), ['..', '.', '.gitignore', '_meta.yaml']));

        if (0 === \count($examples)) {
            throw new \RuntimeException(\sprintf('Topic "%s" doesn\'t have any example, there should be at least one example in path "%s".', $topic, $path));
        }

        $priorities = [];

        foreach ($examples as $example) {
            $meta = $this->readMeta(\sprintf('%s/topics/%s/%s', \realpath($this->examplesPath), $topic, $example));
            $priorities[$example] = $meta['priority'];
        }

        \asort($priorities);

        foreach (\array_keys($priorities) as $example) {
            $meta = $this->readMeta(\sprintf('%s/topics/%s/%s', \realpath($this->examplesPath), $topic, $example));

            if ($meta['hidden']) {
                unset($priorities[$example]);
            }
        }

        return \array_keys($priorities);
    }

    /**
     * @return array<string, array{name: string, arguments: array<string, bool|int|string>}>
     */
    public function examplesNavigation(string $topic) : array
    {
        $navigation = [];

        foreach ($this->topics() as $nextTopic) {

            if ($nextTopic !== $topic) {
                continue;
            }

            $examples = $this->examples($nextTopic);

            foreach ($examples as $nextExample) {

                $options = $this->options($nextTopic, $nextExample);

                if (\count($options) > 0) {
                    $firstOption = type_string()->assert(\reset($options));
                    $navigation[$nextExample] = [
                        'name' => 'example_option',
                        'arguments' => ['topic' => $nextTopic, 'example' => $nextExample, 'option' => $firstOption],
                    ];
                } else {
                    $navigation[$nextExample] = [
                        'name' => 'example',
                        'arguments' => ['topic' => $nextTopic, 'example' => $nextExample],
                    ];
                }
            }
        }

        return $navigation;
    }

    public function firstExample(string $topic) : string
    {
        $examples = $this->examples($topic);

        return type_string()->assert(\reset($examples));
    }

    public function firstOption(string $topic, string $example) : ?string
    {
        $options = $this->options($topic, $example);

        return \count($options) > 0 ? type_string()->assert(\reset($options)) : null;
    }

    /**
     * Returns all visible options for a given example, sorted by priority.
     * Returns empty array if example is standalone (2-level structure).
     *
     * @return array<string>
     */
    public function options(string $topic, string $example) : array
    {
        $path = \sprintf('%s/topics/%s/%s', \realpath($this->examplesPath), $topic, $example);

        if (false === \file_exists($path)) {
            throw new \RuntimeException(\sprintf('Example "%s" in topic "%s" doesn\'t exist, it should be located in path: "%s".', $example, $topic, $path));
        }

        if (\file_exists(\sprintf('%s/code.php', $path))) {
            return [];
        }

        $items = \array_values(\array_diff(\scandir($path), ['..', '.', '.gitignore', '_meta.yaml']));
        $options = [];

        foreach ($items as $item) {
            $itemPath = \sprintf('%s/%s', $path, $item);

            if (\is_dir($itemPath) && \file_exists(\sprintf('%s/code.php', $itemPath))) {
                $options[] = $item;
            }
        }

        if (0 === \count($options)) {
            return [];
        }

        $priorities = [];

        foreach ($options as $option) {
            $meta = $this->readMeta(\sprintf('%s/%s', $path, $option));
            $priorities[$option] = $meta['priority'];
        }

        \asort($priorities);

        foreach (\array_keys($priorities) as $option) {
            $meta = $this->readMeta(\sprintf('%s/%s', $path, $option));

            if ($meta['hidden']) {
                unset($priorities[$option]);
            }
        }

        return \array_keys($priorities);
    }

    /**
     * @return array<string, array{name: string, arguments: array<string, bool|int|string>}>
     */
    public function optionsNavigation(string $topic, string $example) : array
    {
        $navigation = [];

        foreach ($this->topics() as $nextTopic) {

            if ($nextTopic !== $topic) {
                continue;
            }

            $examples = $this->examples($nextTopic);

            foreach ($examples as $nextExample) {

                if ($nextExample !== $example) {
                    continue;
                }

                $options = $this->options($nextTopic, $nextExample);

                foreach ($options as $nextOption) {
                    $navigation[$nextOption] = [
                        'name' => 'example_option',
                        'arguments' => ['topic' => $nextTopic, 'example' => $example, 'option' => $nextOption],
                    ];
                }
            }
        }

        return $navigation;
    }

    /**
     * @return array{name: string, arguments: array<string, string>}
     */
    public function playgroundUrl(string $topic, string $example, ?string $option = null) : array
    {
        if ($option !== null) {
            return [
                'name' => 'example_option_playground',
                'arguments' => [
                    'topic' => $topic,
                    'example' => $example,
                    'option' => $option,
                ],
            ];
        }

        return [
            'name' => 'example_playground',
            'arguments' => [
                'topic' => $topic,
                'example' => $example,
            ],
        ];
    }

    /**
     * @return array<string>
     */
    public function topics() : array
    {
        $path = \sprintf('%s/topics', \realpath($this->examplesPath));

        if (false === \file_exists($path)) {
            throw new \RuntimeException(\sprintf('Topics root directory doesn\'t exists, it should be located in path: "%s".', $path));
        }

        $topics = \array_values(\array_diff(\scandir($path), ['..', '.']));

        if (0 === \count($topics)) {
            throw new \RuntimeException(\sprintf('Topics root directory doesn\'t have any topic, there should be at least one topic in path "%s".', $path));
        }

        $priorities = [];

        foreach ($topics as $topic) {
            $meta = $this->readMeta(\sprintf('%s/topics/%s', \realpath($this->examplesPath), $topic));
            $priorities[$topic] = $meta['priority'];
        }

        \asort($priorities);

        foreach (\array_keys($priorities) as $topic) {
            $meta = $this->readMeta(\sprintf('%s/topics/%s', \realpath($this->examplesPath), $topic));

            if ($meta['hidden']) {
                unset($priorities[$topic]);
            }
        }

        return \array_keys($priorities);
    }

    /**
     * @return array<string, array{name: string, arguments: array<string, bool|int|string>}>
     */
    public function topicsNavigation() : array
    {
        $navigation = [];

        foreach ($this->topics() as $topic) {
            $examples = $this->examples($topic);
            $firstExample = type_string()->assert(\reset($examples));

            $options = $this->options($topic, $firstExample);

            if (\count($options) > 0) {
                $firstOption = type_string()->assert(\reset($options));
                $navigation[$topic] = ['name' => 'example_option', 'arguments' => ['topic' => $topic, 'example' => $firstExample, 'option' => $firstOption]];
            } else {
                $navigation[$topic] = ['name' => 'example', 'arguments' => ['topic' => $topic, 'example' => $firstExample]];
            }
        }

        return $navigation;
    }

    private function examplePath(string $topic, string $example, ?string $option = null) : string
    {
        if ($option !== null) {
            return \sprintf('%s/topics/%s/%s/%s', \realpath($this->examplesPath), $topic, $example, $option);
        }

        if ($this->hasOptions($topic, $example)) {
            $options = $this->options($topic, $example);

            if (0 === \count($options)) {
                throw new \RuntimeException(\sprintf('Example "%s" in topic "%s" has no valid options.', $example, $topic));
            }

            $firstOption = \current($options);

            return \sprintf('%s/topics/%s/%s/%s', \realpath($this->examplesPath), $topic, $example, $firstOption);
        }

        return \sprintf('%s/topics/%s/%s', \realpath($this->examplesPath), $topic, $example);
    }

    private function hasOptions(string $topic, string $example) : bool
    {
        $path = \sprintf('%s/topics/%s/%s', \realpath($this->examplesPath), $topic, $example);

        if (\file_exists(\sprintf('%s/code.php', $path))) {
            return false;
        }

        if (!\file_exists($path)) {
            return false;
        }

        $items = \scandir($path);

        if (\count($items)) {
            foreach ($items as $item) {
                if ($item === '.' || $item === '..') {
                    continue;
                }

                $itemPath = \sprintf('%s/%s', $path, $item);

                if (\is_dir($itemPath) && \file_exists(\sprintf('%s/code.php', $itemPath))) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Read _meta.yaml file and return priority and hidden values.
     *
     * @return array{priority: int, hidden: bool}
     */
    private function readMeta(string $path) : array
    {
        $metaPath = $path . '/_meta.yaml';

        if (!\file_exists($metaPath)) {
            return ['priority' => 99, 'hidden' => false];
        }

        $content = \file_get_contents($metaPath);
        $meta = Yaml::parse($content);

        return [
            'priority' => $meta['priority'] ?? 99,
            'hidden' => $meta['hidden'] ?? false,
        ];
    }
}
