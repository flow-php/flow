<?php

declare(strict_types=1);

namespace Flow\Website\Service;

final class Examples
{
    public function __construct(private readonly string $examplesPath)
    {
    }

    public function code(string $topic, string $example) : string
    {
        $path = \sprintf('%s/topics/%s/%s/code.php', \realpath($this->examplesPath), $topic, $example);

        if (false === \file_exists($path)) {
            throw new \RuntimeException(\sprintf('Code example doesn\'t exists, it should be located in path: "%s".', $path));
        }

        return \file_get_contents($path);
    }

    public function description(string $topic, string $example) : ?string
    {
        $path = \sprintf('%s/topics/%s/%s/description.md', \realpath($this->examplesPath), $topic, $example);

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

    public function output(string $topic, string $example) : ?string
    {
        $path = \sprintf('%s/topics/%s/%s/output.txt', \realpath($this->examplesPath), $topic, $example);

        if (false === \file_exists($path)) {
            return null;
        }

        return \file_get_contents($path);
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
}
