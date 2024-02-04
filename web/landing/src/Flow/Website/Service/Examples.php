<?php declare(strict_types=1);

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

    /**
     * @return string[]
     */
    public function examples(string $topic) : array
    {
        $path = \sprintf('%s/topics/%s', \realpath($this->examplesPath), $topic);

        if (false === \file_exists($path)) {
            throw new \RuntimeException(\sprintf('Topic "%s" doesn\'t exists, it should be located in path: "%s".', $topic, $path));
        }

        $examples = \array_diff(\scandir($path), ['..', '.']);

        if (0 === \count($examples)) {
            throw new \RuntimeException(\sprintf('Topic "%s" doesn\'t have any example, there should be at least one example in path "%s".', $topic, $path));
        }

        return $examples;
    }

    /**
     * @return string[]
     */
    public function topics() : array
    {
        $path = \sprintf('%s/topics', \realpath($this->examplesPath));

        if (false === \file_exists($path)) {
            throw new \RuntimeException(\sprintf('Topics root directory doesn\'t exists, it should be located in path: "%s".', $path));
        }

        $topics = \array_diff(\scandir($path), ['..', '.']);

        if (0 === \count($topics)) {
            throw new \RuntimeException(\sprintf('Topics root directory doesn\'t have any topic, there should be at least one topic in path "%s".', $path));
        }

        return $topics;
    }
}
