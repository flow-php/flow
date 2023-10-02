<?php declare(strict_types=1);

const __FLOW_DATA__ = __DIR__ . '/data';
const __FLOW_OUTPUT__ = __DIR__ . '/output';
const __FLOW_VAR__ = __DIR__ . '/var';
const __FLOW_VAR_RUN__ = __DIR__ . '/var/run';
const __FLOW_SRC__ = __DIR__ . '/../src';

if (!\is_dir(__FLOW_VAR__)) {
    \mkdir(__FLOW_VAR__);
}

if (!\is_dir(__FLOW_VAR_RUN__)) {
    \mkdir(__FLOW_VAR_RUN__);
}
