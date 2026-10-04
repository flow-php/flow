A batch stores one column per schema definition, and the backend set with
`config_builder()->backend()` builds every one of them. `AdaptiveBackend`, the default, picks
`RustBackend` when the flow_php extension is loaded and `PhpBackend` otherwise. Set `PhpBackend`
to opt out of the extension; both give the same values.
