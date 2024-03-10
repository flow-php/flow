<?php

declare(strict_types=1);

if (!\function_exists('dd')) {
    function dd(...$args) : void
    {
        \var_dump(...$args);

        exit(1);
    }
}

if (!\function_exists('dj')) {
    function dj(...$args) : void
    {
        $output = [];

        foreach ($args as $arg) {
            $output[] = \json_encode($arg);
        }

        \var_dump($output);
    }
}

if (!\function_exists('djd')) {
    function djd(...$args) : void
    {

        \dj(...$args);

        exit(1);
    }
}
