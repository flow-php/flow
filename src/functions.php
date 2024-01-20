<?php

if (!\function_exists('dd')) {
    function dd(...$args)
    {
        \var_dump(...$args);

        die(1);
    }
}

if (!\function_exists('dj')) {
    function dj(...$args)
    {
        $output = [];
        foreach ($args as $arg) {
            $output[] = \json_encode($arg);
        }

        var_dump($output);
    }
}

if (!\function_exists('djd')) {
    function djd(...$args)
    {

        dj(...$args);

        exit(1);
    }
}