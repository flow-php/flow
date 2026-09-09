An extractor yields rows, a loader writes them. That is the whole pipeline.

`from_array()` is the source, `to_output()` the destination, and `run()` is what makes anything
happen - everything before it only builds the plan.
