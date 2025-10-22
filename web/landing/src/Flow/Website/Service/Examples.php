<?php

declare(strict_types=1);

namespace Flow\Website\Service;

use function Flow\Types\DSL\type_string;
use Flow\Website\Service\Example\Output;

final class Examples
{
    public function __construct(private readonly string $examplesPath)
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
     * @throws \JsonException
     */
    public function composer(string $topic, string $example, ?string $option = null) : string
    {
        if ($option !== null) {
            $path = \sprintf('%s/topics/%s/%s/%s/composer.json', \realpath($this->examplesPath), $topic, $example, $option);
        } elseif ($this->hasOptions($topic, $example)) {
            $options = $this->options($topic, $example);

            if (0 === \count($options)) {
                throw new \RuntimeException(\sprintf('Example "%s" in topic "%s" has no valid options.', $example, $topic));
            }
            $firstOption = \current($options);
            $path = \sprintf('%s/topics/%s/%s/%s/composer.json', \realpath($this->examplesPath), $topic, $example, $firstOption);
        } else {
            $path = \sprintf('%s/topics/%s/%s/composer.json', \realpath($this->examplesPath), $topic, $example);
        }

        if (false === \file_exists($path)) {
            throw new \RuntimeException(\sprintf('Composer file doesn\'t exists, it should be located in path: "%s".', $path));
        }

        $composer = \json_decode(\file_get_contents($path), true, 512, \JSON_THROW_ON_ERROR);

        if (\array_key_exists('archive', $composer)) {
            unset($composer['archive']);
        }

        return \json_encode($composer, \JSON_PRETTY_PRINT | \JSON_UNESCAPED_SLASHES);
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

        $examples = \array_values(\array_diff(\scandir($path), ['..', '.', '.gitignore', 'priority.txt']));

        if (0 === \count($examples)) {
            throw new \RuntimeException(\sprintf('Topic "%s" doesn\'t have any example, there should be at least one example in path "%s".', $topic, $path));
        }

        $priorities = [];

        foreach ($examples as $example) {
            $path = \sprintf('%s/topics/%s/%s/priority.txt', \realpath($this->examplesPath), $topic, $example);

            if (false === \file_exists($path)) {
                $priorities[$example] = 99;
            } else {
                $priorities[$example] = (int) \file_get_contents($path);
            }
        }

        \asort($priorities);

        foreach (\array_keys($priorities) as $example) {
            $isHidden = \file_exists(\sprintf('%s/topics/%s/%s/hidden.txt', \realpath($this->examplesPath), $topic, $example));

            if ($isHidden) {
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

        $items = \array_values(\array_diff(\scandir($path), ['..', '.', '.gitignore', 'priority.txt', 'hidden.txt']));
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
            $priorityPath = \sprintf('%s/%s/priority.txt', $path, $option);
            $priorities[$option] = \file_exists($priorityPath) ? (int) \file_get_contents($priorityPath) : 99;
        }

        \asort($priorities);

        foreach (\array_keys($priorities) as $option) {
            $isHidden = \file_exists(\sprintf('%s/%s/hidden.txt', $path, $option));

            if ($isHidden) {
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

    public function output(string $topic, string $example, ?string $option = null) : ?Output
    {
        if ($option !== null) {
            $folder = \sprintf('%s/topics/%s/%s/%s', \realpath($this->examplesPath), $topic, $example, $option);
        } elseif ($this->hasOptions($topic, $example)) {
            $options = $this->options($topic, $example);

            if (0 === \count($options)) {
                return null;
            }
            $firstOption = \current($options);
            $folder = \sprintf('%s/topics/%s/%s/%s', \realpath($this->examplesPath), $topic, $example, $firstOption);
        } else {
            $folder = \sprintf('%s/topics/%s/%s', \realpath($this->examplesPath), $topic, $example);
        }

        $paths = \glob(
            \sprintf(
                '{%s/output.{txt,xml,csv,json},%s/output.*.{txt,xml,csv,json}}',
                $folder,
                $folder
            ),
            GLOB_BRACE
        );

        if (!\count($paths)) {
            return null;
        }

        $content = '';

        foreach ($paths as $path) {
            $content .= \file_get_contents($path);

            if (\count($paths) > 1) {
                $content .= \PHP_EOL;
            }
        }

        if (\count($paths) > 1) {
            $extension = 'shell';
        } else {
            $extension = \pathinfo($paths[0], \PATHINFO_EXTENSION);

            if ($extension === 'txt') {
                $extension = 'shell';
            }
        }

        return new Output($content, $extension);
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
            $path = \sprintf('%s/topics/%s/priority.txt', \realpath($this->examplesPath), $topic);

            if (false === \file_exists($path)) {
                $priorities[$topic] = 99;
            } else {
                $priorities[$topic] = (int) \file_get_contents($path);
            }
        }

        \asort($priorities);

        foreach (\array_keys($priorities) as $topic) {
            $isHidden = \file_exists(\sprintf('%s/topics/%s/hidden.txt', \realpath($this->examplesPath), $topic));

            if ($isHidden) {
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
}
